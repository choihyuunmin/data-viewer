import os
import logging
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dataviewer
from logging_config import get_logging_config
from logging.config import dictConfig

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "./logs/app.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

dictConfig(get_logging_config(LOG_FILE_PATH, LOG_LEVEL))
logger = logging.getLogger(__name__)

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
    logger.info("Health check")
    return {"message": "DataViewer API is running"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_config=get_logging_config(LOG_FILE_PATH, LOG_LEVEL),
    )