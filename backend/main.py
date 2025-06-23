import os
import re
import time

import numpy as np
import duckdb
import uvicorn

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel, field_validator

app = FastAPI()

# 보안 설정
security = HTTPBearer()
MAX_QUERY_LENGTH = 1000
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX_REQUESTS = 100

# Rate limiting을 위한 저장소
request_counts = {}

# 허용된 쿼리 키워드 (화이트리스트)
ALLOWED_KEYWORDS = {
    'select', 'from', 'where', 'order', 'by', 'limit', 'offset', 'group', 'having',
    'count', 'sum', 'avg', 'min', 'max', 'distinct', 'as', 'and', 'or', 'not',
    'in', 'between', 'like', 'is', 'null', 'desc', 'asc'
}

class QueryRequest(BaseModel):
    query: str
    file_path: str
    page: int
    page_size: int

    @field_validator('query')
    def validate_query(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Query cannot be empty')
        if len(v) > MAX_QUERY_LENGTH:
            raise ValueError(f'Query too long. Maximum length is {MAX_QUERY_LENGTH}')
        
        query_lower = v.lower()
        
        dangerous_keywords = [
            'drop', 'delete', 'insert', 'update', 'create', 'alter', 'truncate',
            'exec', 'execute', 'union', 'script', 'javascript', 'eval', 'system'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in query_lower:
                raise ValueError(f'Dangerous keyword "{keyword}" is not allowed')
        
        # 허용된 키워드만 사용하는지 확인
        words = re.findall(r'\b\w+\b', query_lower)
        for word in words:
            if word not in ALLOWED_KEYWORDS and not word.isdigit():
                raise ValueError(f'Keyword "{word}" is not allowed')
        
        return v

    @field_validator('file_path')
    def validate_file_path(cls, v):
        if not v or '..' in v or '/' in v or '\\' in v:
            raise ValueError('Invalid file path')
        return v

def check_rate_limit(request: Request) -> bool:
    """Rate limiting 검증"""
    client_ip = request.client.host
    current_time = time.time()
    
    if client_ip not in request_counts:
        request_counts[client_ip] = {'count': 0, 'window_start': current_time}
    
    # 윈도우가 지났으면 리셋
    if current_time - request_counts[client_ip]['window_start'] > RATE_LIMIT_WINDOW:
        request_counts[client_ip] = {'count': 0, 'window_start': current_time}
    
    # 요청 수 증가
    request_counts[client_ip]['count'] += 1
    
    # 제한 확인
    if request_counts[client_ip]['count'] > RATE_LIMIT_MAX_REQUESTS:
        return False
    
    return True

def sanitize_file_path(file_path: str) -> str:
    # 상대 경로 제거
    file_path = os.path.normpath(file_path)
    if file_path.startswith('..') or file_path.startswith('/'):
        raise ValueError('Invalid file path')
    return file_path

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

def wrap_column_name(column):
    return f'"{column}"'

def extract_total_count(con, query: str) -> int:
    return con.execute(f"SELECT COUNT(*) AS total FROM ({query}) AS subquery").fetchone()[0]

def calculate_distributions(query):
    distributions = {}
    with duckdb.connect() as con:
        df = con.execute(query).pl()
        
        for col in df.columns:
            dtype_str = str(df.schema[col]).lower()

            if dtype_str in ['string', 'varchar', 'char', 'text', 'boolean', 'utf8']:
                
                value_counts = df[col].value_counts().sort("count", descending=True)
                
                labels = value_counts[col].to_list()
                counts = value_counts["count"].to_list()

                distributions[col] = {
                    'type': 'categorical',
                    'counts': counts,
                    'labels': labels
                }
            else:
                series = df[col].drop_nulls().to_numpy()
                if len(series) == 0:
                    continue

                min_val = int(np.floor(series.min()))
                max_val = int(np.ceil(series.max()))

                # min_val과 max_val이 같은 경우 (bin이 한 개일 때)
                if min_val == max_val:
                    bin_edges = np.array([min_val, max_val + 1])
                    counts = np.array([len(series)])
                    labels = [str(min_val)]
                else:
                    bin_edges = np.linspace(min_val, max_val, num=21)
                    bin_edges = np.unique(np.round(bin_edges).astype(int))
                    counts, _ = np.histogram(series, bins=bin_edges)
                    labels = [str(b) for b in bin_edges[:-1]]

                distributions[col] = {
                    'type': 'numeric',
                    'counts': counts.tolist(),
                    'labels': labels
                }

    return distributions

@app.post("/dataviewer/init")
async def get_file(file: UploadFile = File(...), request: Request = None):
    # Rate limiting 검증
    if request and not check_rate_limit(request):
        raise HTTPException(status_code=429, detail="Too many requests")
    
    try:
        file_name = file.filename.split('.')[0]
        # 파일명 정리
        file_name = re.sub(r'[^a-zA-Z0-9_-]', '', file_name)
        if not file_name:
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        file_path = os.path.join(os.getcwd(), "..", "dataset", f"{file_name}.parquet")
        
        # 파일 경로 검증
        file_path = sanitize_file_path(file_path)
        
        with duckdb.connect() as con:
            base_query = f"SELECT * FROM read_parquet('{file_path}')"
            sample_query = f"{base_query} LIMIT 10"
            preview = con.execute(sample_query).pl()

            total_count = extract_total_count(con, base_query)
            columns = preview.columns
            
            distributions = calculate_distributions(base_query)
        
        return JSONResponse({
            "file_path": file_path,
            "columns": columns,
            "tableData": preview.to_dicts(),
            "distributions": distributions,
            "total": total_count
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dataviewer/page")
async def get_page(request: QueryRequest, http_request: Request = None):
    # Rate limiting 검증
    if http_request and not check_rate_limit(http_request):
        raise HTTPException(status_code=429, detail="Too many requests")
    
    try:
        query = request.query.lower()
        query = query.replace('from data', f"FROM read_parquet('{request.file_path}')")

        with duckdb.connect() as con:
            offset = (request.page - 1) * request.page_size
            query = f"{query} LIMIT {request.page_size} OFFSET {offset}"
            result = con.execute(query).pl()
        
        return JSONResponse({
            "tableData": result.to_dicts()
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dataviewer/query")
async def execute_query(request: QueryRequest, http_request: Request = None):
    # Rate limiting 검증
    if http_request and not check_rate_limit(http_request):
        raise HTTPException(status_code=429, detail="Too many requests")
    
    try:
        query = request.query.lower()
        query = query.replace('from data', f"FROM read_parquet('{request.file_path}')")
        
        with duckdb.connect() as con:
            sample_query = f"{query} LIMIT 10"
            preview = con.execute(sample_query).pl()

            total_count = extract_total_count(con, query)
            columns = preview.columns
            
            distributions = calculate_distributions(query)
        
        return JSONResponse({
            "columns": columns,
            "tableData": preview.to_dicts(),
            "distributions": distributions,
            "total": total_count
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 