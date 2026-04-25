from __future__ import annotations

from typing import Any, Dict

from src.database import DatabaseFactory, DuckDB


class Loader:
    def __init__(self, engine: str, database: str, duckdb_path: str) -> None:
        kwargs = {"database": database}
        self.db = DatabaseFactory.build(engine=engine, **kwargs)
        self.duckdb = DuckDB(database=duckdb_path)


    def _build_select_query(
        self,
        table: str,
        columns: list[str] | None = None,
        filter: Dict[str, Any] | None = None,
    ) -> str:
        cols = ", ".join(columns) if columns else "*"

        where = ""
        if filter:
            clauses = " AND ".join([f"{k} = {v}" for k, v in filter.items()])
            where = f" WHERE {clauses}"

        return f"SELECT {cols} FROM {table}{where}"


    def _load_parquet_into_duckdb(self, table: str, path: str) -> None:
        self.duckdb.insert_data(table_name=table, data=str(path))


    def load(
        self,
        table: str,
        columns: list[str] | None = None,
        filter: Dict[str, Any] | None = None,
        output: str = "file.parquet",
        chunk_size: int = 1000,
        stream: bool = False,
    ) -> None:
        query = self._build_select_query(table, columns, filter)

        path = self.db.extract_to_parquet(
            query=query, output=output, chunk_size=chunk_size, stream=stream
        )

        path = str(path)

        self._load_parquet_into_duckdb(table, path)
