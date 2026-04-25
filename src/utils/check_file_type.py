import os
from .logger import get_logger

logger = get_logger(__name__)

def get_file_type(filepath: str) -> str:
    logger.debug("Detecting file type for '%s'", filepath)
    _, ext = os.path.splitext(filepath)
    return ext.lstrip(".").lower() if ext else "unknown"


def is_parquet_file(filepath: str) -> bool:
    logger.debug("Checking if '%s' is parquet", filepath)
    return get_file_type(filepath) == "parquet"


def is_csv_file(filepath: str) -> bool:
    logger.debug("Checking if '%s' is csv", filepath)
    return get_file_type(filepath) == "csv"