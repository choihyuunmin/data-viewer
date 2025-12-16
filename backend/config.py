import os

MAX_QUERY_LENGTH = 1000
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX_REQUESTS = 100

# 허용된 쿼리 키워드 (화이트리스트)
ALLOWED_KEYWORDS = {
    'select', 'from', 'where', 'order', 'by', 'limit', 'offset', 'group', 'having',
    'count', 'sum', 'avg', 'min', 'max', 'distinct', 'as', 'and', 'or', 'not',
    'in', 'between', 'like', 'is', 'null', 'desc', 'asc'
}

DANGEROUS_KEYWORDS = [
    'drop', 'delete', 'insert', 'update', 'create', 'alter', 'truncate',
    'exec', 'execute', 'union', 'script', 'javascript', 'eval', 'system'
]

# 환경 설정
ENVIRONMENT = os.getenv("DATAVIEWER_ENV", "dev").lower()

if ENVIRONMENT == "prod":
    MINIO_ENDPOINT = ""
    MINIO_ACCESS_KEY = ""
    MINIO_SECRET_KEY = ""
    NAS_ROOT_PATH = ""
elif ENVIRONMENT == "":
    MINIO_ENDPOINT = ""
    MINIO_ACCESS_KEY = ""
    MINIO_SECRET_KEY = ""
    NAS_ROOT_PATH = ""
else:
    MINIO_ENDPOINT = ""
    MINIO_ACCESS_KEY = ""
    MINIO_SECRET_KEY = ""
    NAS_ROOT_PATH = "/Users/choi/Downloads/krihs-nas"