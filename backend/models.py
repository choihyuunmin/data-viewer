import re
from pydantic import BaseModel, field_validator
from config import MAX_QUERY_LENGTH, DANGEROUS_KEYWORDS

class LoadDatasetRequest(BaseModel):
    bucket_name: str
    file_name: str

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