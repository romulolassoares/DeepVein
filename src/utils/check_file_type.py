import os

def get_file_type(filepath: str) -> str:
    _, ext = os.path.splitext(filepath)
    return ext.lstrip(".").lower() if ext else "unknown"


def is_parquet_file(filepath: str) -> bool:
    return get_file_type(filepath) == "parquet"


def is_csv_file(filepath: str) -> bool:
    return get_file_type(filepath) == "csv"