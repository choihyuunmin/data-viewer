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

# --- MinIO 설정 ---
MINIO_ENDPOINT = "192.168.105.41:32000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

NAS_ROOT_PATH = "/DATA1/krihs-file"