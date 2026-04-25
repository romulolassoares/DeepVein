from pathlib import Path
from typing import Any, Dict, List

from .models import Query
from .store import Store
from src.utils import get_logger

logger = get_logger(__name__)

class QueryRegistry:
    def __init__(self, db_path: str = ":memory:") -> None:
        logger.info("Initializing query registry with path '%s'", db_path)
        self._store = Store(db_path)

    
    def add(self, entry: Query) -> None:
        logger.info("Adding query '%s' to registry", entry.id)
        self._store.upsert(entry)

    
    def get(self, query_id: str) -> Query:
        logger.info("Getting query '%s' from registry", query_id)
        entry = self._store.get(query_id)
        
        if entry is None:
            logger.exception("Query '%s' not found in registry", query_id)
            raise KeyError(f"Query {query_id} not found in registry.")

        logger.info("Query '%s' retrieved from registry", query_id)
        return entry


    def update(self, entry: Query) -> None:
        logger.info("Updating query '%s' in registry", entry.id)
        return self._store.upsert(entry)


    def delete(self, query_id: str) -> bool:
        logger.info("Deleting query '%s' from registry", query_id)
        deleted = self._store.delete(query_id)
        logger.info("Delete query '%s' result: %s", query_id, deleted)
        return deleted

    
    def get_by_group(self, group_name: str) -> List[Query]:
        logger.info("Getting queries from group '%s'", group_name)
        return self._store.get_by_group(group_name)

    def close(self) -> None:
        logger.info("Closing query registry")
        self._store.close()

    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        self.close()