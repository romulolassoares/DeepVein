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


    def insert(self, entry: Query) -> None:        
        with self._conn as conn:
            params = json.dumps(entry.params)
            groups = "§".join(entry.groups)
            conn.execute(
                "INSERT OR REPLACE INTO queries (id, sql, params, groups) VALUES (?, ?, ?, ?)",
                (entry.id, entry.sql, params, groups)
            )

    
    def update(self, entry: Query) -> None:
        with self._conn as conn:
            params = json.dumps(entry.params)
            groups = "§".join(entry.groups)
            conn.execute(
                "UPDATE queries SET sql = ?, params = ?, groups = ? WHERE id = ?",
                (entry.sql, params, groups, entry.id)
            )


    def delete(self, query_id: str) -> bool:
        with self._conn as conn:
            result = conn.execute(
                "DELETE FROM queries where id = ?",
                (query_id,)
            )
        return result.rowcount > 0


    def get(self, query_id: str) -> Query:
        with self._conn as conn:
            result = conn.execute(
                "SELECT id, sql, params, groups FROM queries WHERE id = ?",
                (query_id,)
            )
            row = result.fetchone()
        if row is None:
            return None
        return self._convert_to_query(row)


    def get_by_group(self, group_name: str) -> List[Query]:
        with self._conn as conn:
            result = conn.execute(
                """
                SELECT id, sql, params, groups
                FROM queries
                WHERE groups LIKE ?
                """,
                (f"%{group_name}%",)
            )
        rows = result.fetchall()
        return [self._convert_to_query(row) for row in rows]


    def get_groups(self) -> List[str]:
        with self._conn as conn:
            result = conn.execute(
                "SELECT DISTINCT groups FROM queries"
            )
        rows = result.fetchall()
        distinct_values = {
            value.strip()
            for row in rows
            for value in row[0].split("§")
            if value.strip()
        }
        return sorted(distinct_values)

    
    def _convert_to_query(self, row: sqlite3.Row) -> Query:
        query = Query(
            id=row["id"],
            sql=row["sql"],
            params=json.loads(row["params"]),
            groups=row["groups"].split("§")
        )
        return query
        

    def close(self) -> None:
        self._conn.close()
    

    def __enter__(self):
        return self


    def __exit__(self, *_):
        self.close()
