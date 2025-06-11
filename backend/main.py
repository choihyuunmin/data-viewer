import os
import io
import uuid
from typing import Dict

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import uvicorn

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 세션별 데이터프레임 저장소
session_data: Dict[str, str] = {}

class QueryRequest(BaseModel):
    query: str
    session_id: str
    page: int
    page_size: int

class SessionResponse(BaseModel):
    session_id: str

def wrap_column_name(column: str) -> str:
    return f'"{column}"'

def calculate_distributions(file_path, columns):
    distributions = {}
    con = duckdb.connect()

    for column in columns:
        wrapped_column = wrap_column_name(column)
        try:
            col_type = con.execute(f"DESCRIBE SELECT {wrapped_column} FROM read_parquet('{file_path}')").df()['column_type'][0]
            print(f"Column: {column}, Type: {col_type}")

            if col_type == 'VARCHAR':
                # 범주형 열 처리
                query = f"""
                SELECT {wrapped_column}, COUNT(*) AS count
                FROM read_parquet('{file_path}')
                GROUP BY {wrapped_column}
                ORDER BY count DESC
                """
                result = con.execute(query).df()
                distributions[column] = {
                    'type': 'categorical',
                    'counts': result['count'].tolist(),
                    'labels': result[column].tolist()
                }
            else:
                # 수치형 열 처리 (INTEGER, FLOAT, DOUBLE 등)
                query = f"""
                SELECT histogram({wrapped_column}) AS hist
                FROM read_parquet('{file_path}')
                """
                hist = con.execute(query).df()['hist'][0]
                distributions[column] = { 
                    'type': 'numeric',
                    'counts': list(hist.values()),
                    'labels': [str(k) for k in hist.keys()]
                }
        except Exception as e:
            print(f"Error processing column {column}: {str(e)}")
            continue
    
    return distributions

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    session_id = str(uuid.uuid4())
    file_name = file.filename.split('.')[0]
    file_path = os.path.join(os.getcwd(), "..", "dataset", f"{file_name}.parquet")
    
    query = f"SELECT * FROM read_parquet('{file_path}') LIMIT 10"
    preview = duckdb.sql(query).df().to_dict(orient="records")

    total_count = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()[0]
    columns = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()['column_name'].tolist()
    session_data[session_id] = file_path
    distributions = calculate_distributions(file_path, columns)
    
    return JSONResponse({
        "session_id": session_id,
        "columns": columns,
        "preview": preview,
        "distributions": distributions,
        "total": total_count
    })

@app.post("/page")
async def execute_query(request: QueryRequest):
    if request.session_id not in session_data:
        raise HTTPException(status_code=400, detail="유효하지 않은 세션입니다.")
    
    file_path = session_data[request.session_id]
    
    # 페이징 쿼리
    offset = (request.page - 1) * request.page_size
    query = f"{request.query} LIMIT {request.page_size} OFFSET {offset}"
    result = duckdb.sql(query.replace('data', f"read_parquet('{file_path}')")).df()
    
    return JSONResponse({
        "columns": result.columns.tolist(),
        "data": result.to_dict(orient="records")
    })

@app.post("/query")
async def execute_duck_query(request: QueryRequest):
    file_path = session_data[request.session_id]
    
    con = duckdb.connect()
    
    # 전체 결과 수를 가져오기 위한 쿼리
    query = request.query.replace('data', f"read_parquet('{file_path}')")
    count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
    total_count = duckdb.sql(count_query).fetchone()[0]
    
    # 페이징 쿼리
    offset = (request.page - 1) * request.page_size
    query = f"{request.query} LIMIT {request.page_size} OFFSET {offset}"
    result = duckdb.sql(query).df()
    result = result.to_dict(orient="records")
    
    distributions = calculate_distributions(file_path, result.columns.tolist())
    
    return JSONResponse({
        "columns": result.columns.tolist(),
        "data": result.to_dict(orient="records"),
        "distributions": distributions,
        "total": total_count
    })

@app.get("/schema")
async def get_schema(session_id: str):
    if session_id not in session_data:
        raise HTTPException(status_code=400, detail="유효하지 않은 세션입니다.")
    
    file_path = session_data[session_id]
    con = duckdb.connect()
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
    
    return JSONResponse({
        "columns": schema['column_name'].tolist(),
        "dtypes": dict(zip(schema['column_name'], schema['column_type']))
    })

@app.delete("/session/{session_id}")
async def delete_session(session_id: str):
    if session_id in session_data:
        del session_data[session_id]
    return JSONResponse({"message": "세션이 삭제되었습니다."})

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 