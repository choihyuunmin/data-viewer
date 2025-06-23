import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from models import LoadDatasetRequest, QueryRequest
from services.data_service import DataService

router = APIRouter(prefix="/dataviewer", tags=["DataViewer"])
service = DataService()

@router.post("/load_dataset")
async def load_dataset(request: LoadDatasetRequest):
    try:
        return service.get_dataset_details(request.bucket_name, request.file_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {str(e)}")

@router.post("/page")
async def get_page(request: QueryRequest):
    try:
        return service.get_paged_data(request.bucket_name, request.file_name, request.query, request.page, request.page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get page: {str(e)}")

@router.post("/query")
async def execute_query(request: QueryRequest):
    try:
        return service.execute_query(request.bucket_name, request.file_name, request.query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}") 