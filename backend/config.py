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
    DEFAULT_MINIO_ENDPOINT = ""
    DEFAULT_MINIO_ACCESS_KEY = ""
    DEFAULT_MINIO_SECRET_KEY = ""
    DEFAULT_NAS_ROOT_PATH = ""
elif ENVIRONMENT == "":
    DEFAULT_MINIO_ENDPOINT = ""
    DEFAULT_MINIO_ACCESS_KEY = ""
    DEFAULT_MINIO_SECRET_KEY = ""
    DEFAULT_NAS_ROOT_PATH = ""
else:
    DEFAULT_MINIO_ENDPOINT = ""
    DEFAULT_MINIO_ACCESS_KEY = ""
    DEFAULT_MINIO_SECRET_KEY = ""
    DEFAULT_NAS_ROOT_PATH = "/Users/choi/Downloads/krihs-nas"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY)
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY)
NAS_ROOT_PATH = os.getenv("NAS_ROOT_PATH", DEFAULT_NAS_ROOT_PATH)
