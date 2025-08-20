import re
import logging
from fastapi import APIRouter, HTTPException
from models import LoadDatasetRequest, QueryRequest
from services.data_service import DataService

router = APIRouter(prefix="/dataviewer", tags=["DataViewer"])
service = DataService()
logger = logging.getLogger(__name__)

@router.post("/load_dataset")
async def load_dataset(request: LoadDatasetRequest):
    try:
        logger.info("/load_dataset 호출", extra={"bucket": request.bucket_name, "file_name": request.file_name})
        return service.get_dataset_details(request.bucket_name, request.file_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        logger.exception("load_dataset 실패")
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {str(e)}")

@router.post("/page")
async def get_page(request: QueryRequest):
    try:
        logger.info("/page 호출", extra={"bucket": request.bucket_name, "page": request.page, "size": request.page_size})
        return service.get_paged_data(request.query, request.page, request.page_size)
    except Exception as e:
        logger.exception("get_page 실패")
        raise HTTPException(status_code=500, detail=f"Failed to get page: {str(e)}")

@router.post("/query")
async def execute_query(request: QueryRequest):
    try:
        logger.info("/query 호출", extra={"bucket": request.bucket_name})
        return service.execute_query(request.query)
    except Exception as e:
        logger.exception("execute_query 실패")
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")