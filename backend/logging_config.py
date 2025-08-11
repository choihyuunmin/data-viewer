import os
import logging
from logging.config import dictConfig


def get_logging_config(log_file_path: str, log_level: str = "INFO") -> dict:
    # Normalize level
    normalized_level = log_level.upper()
    if normalized_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}:
        normalized_level = "INFO"

    # Timed rotation settings (env-overridable)
    when = os.getenv("LOG_ROTATE_WHEN", "midnight")
    try:
        interval = int(os.getenv("LOG_ROTATE_INTERVAL", "1"))
    except ValueError:
        interval = 1
    try:
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", "14"))
    except ValueError:
        backup_count = 14
    use_utc_env = os.getenv("LOG_USE_UTC", "false").lower()
    use_utc = use_utc_env in {"1", "true", "yes", "y"}

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            },
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": normalized_level,
                "filename": log_file_path,
                "when": when,
                "interval": interval,
                "backupCount": backup_count,
                "utc": use_utc,
                "encoding": "utf-8",
                "formatter": "default",
            },
        },
        "loggers": {
            # Disable uvicorn/fastapi logs from being written anywhere
            "uvicorn": {"handlers": [], "level": "CRITICAL", "propagate": False},
            "uvicorn.error": {"handlers": [], "level": "CRITICAL", "propagate": False},
            "uvicorn.access": {"handlers": [], "level": "CRITICAL", "propagate": False},
            "fastapi": {"handlers": [], "level": "CRITICAL", "propagate": False},
        },
        "root": {
            "handlers": ["file"],
            "level": normalized_level,
        },
    }


