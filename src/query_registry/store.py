import json
import sqlite3
from typing import List

from .models import Query

from src.utils import get_logger

logger = get_logger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS queries (
    id TEXT PRIMARY KEY,
    sql TEXT NOT NULL,
    params TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS groups (
    query_id TEXT NOT NULL REFERENCES queries(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    PRIMARY KEY (query_id, name)
);
"""


class Store:
    def __init__(self, db_path: str = ":memory:") -> None:
        logger.info("Initializing query store with path '%s'", db_path)
        self._path = db_path
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_DDL)
        self._conn.commit()
        logger.info("Query store initialized")


    def upsert(self, entry: Query) -> None:
        logger.info("Upserting query '%s'", entry.id)
        with self._conn as conn:
            params = json.dumps(entry.params)
            
            conn.execute(
                "INSERT OR REPLACE INTO queries (id, sql, params) VALUES (?, ?, ?)",
                (entry.id, entry.sql, params)
            )
            
            conn.execute(
                "DELETE FROM groups WHERE query_id = ?",
                (entry.id,)
            )
            
            conn.executemany(
                "INSERT INTO groups (query_id, name) VALUES (?, ?)",
                [(entry.id, group) for group in entry.groups]
            )
        logger.info("Query '%s' upserted with %s groups", entry.id, len(entry.groups))


    def delete(self, query_id: str) -> bool:
        logger.info("Deleting query '%s'", query_id)
        with self._conn as conn:
            result = conn.execute(
                "DELETE FROM queries where id = ?",
                (query_id,)
            )
        deleted = result.rowcount > 0
        logger.info("Delete result for query '%s': %s", query_id, deleted)
        return deleted


    def get(self, query_id: str) -> Query:
        logger.info("Searching query '%s'", query_id)
        with self._conn as conn:
            result = conn.execute(
                """
                SELECT q.id, q.sql, q.params,
                       COALESCE(
                           (SELECT group_concat(g.name, '§') FROM groups g
                            WHERE g.query_id = q.id),
                           ''
                       ) AS groups
                FROM queries q
                WHERE q.id = ?
                """,
                (query_id,),
            )
            row = result.fetchone()
        if row is None:
            logger.info("Query '%s' not found", query_id)
            return None
        logger.info("Query '%s' found", query_id)
        return self._convert_to_query(row)


    def get_by_group(self, group_name: str) -> List[Query]:
        logger.info("Fetching queries by group '%s'", group_name)
        with self._conn as conn:
            result = conn.execute(
                """
                SELECT DISTINCT q.id, q.sql, q.params,
                       COALESCE(
                           (SELECT group_concat(g2.name, '§') FROM groups g2
                            WHERE g2.query_id = q.id),
                           ''
                       ) AS groups
                FROM queries q
                INNER JOIN groups g ON q.id = g.query_id
                WHERE g.name = ?
                """,
                (group_name,),
            )
        rows = result.fetchall()
        logger.info("Fetched %s queries for group '%s'", len(rows), group_name)
        return [self._convert_to_query(row) for row in rows]


    def get_groups(self) -> List[str]:
        logger.info("Fetching all query groups")
        with self._conn as conn:
            result = conn.execute(
                'SELECT DISTINCT name AS "group" FROM groups'
            )
        rows = result.fetchall()
        logger.info("Fetched %s distinct groups", len(rows))
        return [row["group"] for row in rows]

    
    def _convert_to_query(self, row: sqlite3.Row) -> Query:
        raw = row["groups"] or ""
        groups = [g for g in raw.split("§") if g]
        logger.debug("Converting row to Query object '%s'", row["id"])
        return Query(
            id=row["id"],
            sql=row["sql"],
            params=json.loads(row["params"]),
            groups=groups,
        )
        

    def close(self) -> None:
        logger.info("Closing query store connection")
        self._conn.close()
    

    def __enter__(self):
        return self


    def __exit__(self, *_):
        self.close()
