import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dataviewer

app = FastAPI(
    title="DataViewer API",
    description="An API to view and query Parquet datasets.",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 포함
app.include_router(dataviewer.router)

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "DataViewer API is running"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 