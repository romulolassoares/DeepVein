import json
import sqlite3
from pathlib import Path
from typing import List

from .models import Query

_DDL = """
CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,
    sql TEXT NOT NULL,
    params TEXT NOT NULL DEFAULT '{}',
    groups TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._path = db_path
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_DDL)
        self._conn.commit()


    def close(self) -> None:
        self._conn.close()
    

    def __enter__(self):
        return self


    def __exit__(self, *_):
        self.close()
