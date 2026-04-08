from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from sqlalchemy import Engine, text
from sqlalchemy.engine import create_engine

class Database(ABC):
    def __init__(self):
        self.connection_url = self._build_url()


    @abstractmethod
    def _build_url(self) -> str:
        pass


    def _create_engine(self) -> Engine:
        return create_engine(self.connection_url)


    def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        engine = self._create_engine()

        try:
            with engine.connect() as conn:
                query = text(query)
                params = params or {}

                result = conn.execute(query, params)

                rows = result.fetchall()
                keys = result.keys()

                conn.commit()
        finally:
            engine.dispose()
        
        return [dict(zip(keys, row)) for row in rows]
