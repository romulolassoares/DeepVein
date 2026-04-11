from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional

from sqlalchemy import Engine, text
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils import config

_QUERY_SNIPPET_MAX = 200


class DatabaseConfigurationError(RuntimeError):
    pass


class DatabaseQueryError(RuntimeError):
    pass


def _get_database_config() -> Dict[str, Any]:
    raw = config.get("database")
    if raw is None:
        raise DatabaseConfigurationError(
            'Config must define a "database" key (see config/config.yml.example).'
        )
    return raw

class Database(ABC):
    def __init__(self, database: str) -> None:
        self.config: Dict[str, Any] = _get_database_config()
        self.connection_url = self._build_url(database)
        self._engine: Optional[Engine] = None


    @abstractmethod
    def _build_url(self, database: str) -> str: ...


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
        except SQLAlchemyError as e:
            snippet = query.strip().replace("\n", " ")
            if len(snippet) > _QUERY_SNIPPET_MAX:
                snippet = snippet[:_QUERY_SNIPPET_MAX] + "…"
            raise DatabaseQueryError(
                f"Query failed [sql: {snippet}]"
            ) from e
        finally:
            engine.dispose()

        return [dict(zip(keys, row)) for row in rows]
