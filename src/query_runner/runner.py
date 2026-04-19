"""
Recive a list of Query
For each element execute the query into Database (duckDb)

"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from string import Template
from typing import List

from src.query_registry import Query
from src.database import DuckDB

class Runner:
    def __init__(self) -> None:
        pass

    @classmethod
    def execute(
        cls,
        queries: List[Query],
        database_path: str,
        parallel: bool = False,
        max_workers: int = 4
    ) -> None:
        if parallel:
            return cls()._parallel_runner(queries, database_path, max_workers)
        
        return cls()._simple_runner(queries, database_path)


    def _executor(self, query: Query, database_path: str) -> None:
        db = DuckDB(database=database_path, read_only=True)
        sql = query.render()
        db.execute(sql)


    def _simple_runner(self, queries: List[Query], database_path: str):
        for query in queries:
            self._executor(query, database_path)


    def _parallel_runner(
        self,
        queries: List[Query],
        database_path: str,
        max_workers: int = 4
    ) -> None:
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(self._executor, query, database_path): query
                for query in queries
            }

            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Item {item} failed: {str(e)}")

