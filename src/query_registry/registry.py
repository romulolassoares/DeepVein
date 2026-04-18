from pathlib import Path
from typing import Any, Dict, List

from .models import Query
from .store import Store

class QueryRegistry:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._store = Store(db_path)

    
    def add(self, entry: Query) -> None:
        self._store.upsert(entry)

    
    def get(self, query_id: str) -> Query:
        entry = self._store.get(query_id)
        
        if entry is None:
            raise KeyError(f"Query {query_id} not found in registry.")

        return entry


    def update(self, entry: Query) -> None:
        return self._store.upsert(entry)


    def delete(self, query_id: str) -> bool:
        return self._store.delete(query_id)

    
    def get_by_group(self, group_name: str) -> List[Query]:
        return self._store.get_by_group(group_name)

    def close(self) -> None:
        self._store.close()

    def __enter__(self):
        return self
    
    def __exit__(self, *_):
        self.close()