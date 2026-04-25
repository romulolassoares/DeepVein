from concurrent.futures import ThreadPoolExecutor, as_completed
from string import Template
from typing import List

from src.query_registry import Query
from src.database import DuckDB
from src.utils import get_logger

logger = get_logger(__name__)

class Runner:
    def __init__(self) -> None:
        logger.debug("Runner instance created")

    @classmethod
    def execute(
        cls,
        queries: List[Query],
        database_path: str,
        parallel: bool = False,
        max_workers: int = 4
    ) -> None:
        logger.info(
            "Executing %s queries using %s mode",
            len(queries),
            "parallel" if parallel else "sequential",
        )
        if parallel:
            return cls()._parallel_runner(queries, database_path, max_workers)
        
        return cls()._simple_runner(queries, database_path)


    def _executor(self, query: Query, database_path: str) -> None:
        db = DuckDB(database=database_path, read_only=True)
        sql = query.render()

        logger.debug("Executing query '%s'", query.id)
        
        db.execute(sql)
        logger.debug("Query '%s' executed successfully", query.id)


    def _simple_runner(self, queries: List[Query], database_path: str):
        logger.info("Running %s queries sequentially", len(queries))
        for query in queries:
            self._executor(query, database_path)
        logger.info("Sequential execution finished")


    def _parallel_runner(
        self,
        queries: List[Query],
        database_path: str,
        max_workers: int = 4
    ) -> None:
        logger.info("Running %s queries in parallel with %s workers", len(queries), max_workers)
        with ThreadPoolExecutor(max_workers) as executor:
            futures = {
                executor.submit(self._executor, query, database_path): query
                for query in queries
            }

            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                    logger.debug("Query '%s' completed", item.id)
                except Exception as e:
                    logger.exception("Query '%s' failed: %s", item.id, str(e))
        logger.info("Parallel execution finished")

