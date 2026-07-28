import re
from pydantic import BaseModel, Field, field_validator
from config import MAX_QUERY_LENGTH, DANGEROUS_KEYWORDS

class LoadDatasetRequest(BaseModel):
    bucket_name: str
    file_name: str
    type: str = ""


class MapPreviewRequest(BaseModel):
    bucket_name: str
    file_name: str
    type: str = ""
    layer: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    limit: int = Field(default=1000, ge=1, le=5000)
    simplify_tolerance: float = Field(default=0.0, ge=0.0, le=5.0)
    fields: list[str] | None = None

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, value):
        if value is None:
            return value
        west, south, east, north = value
        if west >= east or south >= north:
            raise ValueError("bbox must be ordered as west, south, east, north")
        if west < -180 or east > 180 or south < -90 or north > 90:
            raise ValueError("bbox must use WGS84 longitude and latitude")
        return value

class QueryRequest(BaseModel):
    query: str
    bucket_name: str
    page: int
    page_size: int

    @field_validator('query')
    def validate_query(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Query cannot be empty')
        if len(v) > MAX_QUERY_LENGTH:
            raise ValueError(f'Query too long. Maximum length is {MAX_QUERY_LENGTH}')
        
        query_lower = v.lower()
        
        for keyword in DANGEROUS_KEYWORDS:
            if keyword in query_lower:
                raise ValueError(f'Dangerous keyword "{keyword}" is not allowed')
        
        return v 

class DownloadRequest(BaseModel):
    query: str
    bucket_name: str

    @field_validator('query')
    def validate_query(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Query cannot be empty')
        if len(v) > MAX_QUERY_LENGTH:
            raise ValueError(f'Query too long. Maximum length is {MAX_QUERY_LENGTH}')

        query_lower = v.lower()

        for keyword in DANGEROUS_KEYWORDS:
            if keyword in query_lower:
                raise ValueError(f'Dangerous keyword "{keyword}" is not allowed')

        return v
