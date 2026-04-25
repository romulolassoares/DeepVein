from typing import Any, ClassVar, Type
from .database import Database
from .sql_server import SQLServerConnection
from src.utils import get_logger

logger = get_logger(__name__)


class DatabaseFactory:
    _registry: ClassVar[dict[str, Type[Database]]] = {
        "sqlserver": SQLServerConnection,
    }

    @classmethod
    def build(cls, engine: str, **kwargs: Any) -> Database:
        key = engine.lower().strip()
        logger.info("Building database engine '%s'", key)
        conn_cls = cls._registry.get(key)
        if conn_cls is None:
            supported = ", ".join(sorted(cls._registry))
            logger.exception("Unsupported database engine '%s'", engine)
            raise ValueError(
                f"Unsupported engine {engine!r}. Supported engines: {supported}"
            )
        logger.info("Database engine '%s' built", key)
        return conn_cls(**kwargs)
