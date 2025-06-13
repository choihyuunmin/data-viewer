import os
import json

import numpy as np
import duckdb
import polars as pl
import uvicorn

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class QueryRequest(BaseModel):
    query: str
    file_path: str
    page: int
    page_size: int

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

@app.post("/init")
async def get_file(file: UploadFile = File(...)):
    file_name = file.filename.split('.')[0]
    file_path = os.path.join(os.getcwd(), "..", "dataset", f"{file_name}.parquet")
    
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

@app.post("/page")
async def get_page(request: QueryRequest):
    query = request.query.lower()
    query = query.replace('from data', f"FROM read_parquet('{request.file_path}')")

    with duckdb.connect() as con:
        offset = (request.page - 1) * request.page_size
        query = f"{query} LIMIT {request.page_size} OFFSET {offset}"
        print(query)
        result = con.execute(query).pl()
        print(result)
        columns = result.columns
    
    return JSONResponse({
        "columns": columns,
        "tableData": result.to_dicts()
    })

@app.post("/query")
async def execute_query(request: QueryRequest):
    if not request.query:
        raise HTTPException(status_code=400, detail="Query is required")
    
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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 