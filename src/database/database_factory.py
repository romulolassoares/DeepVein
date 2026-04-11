from typing import Any, ClassVar, Type
from .database import Database
from .sql_server import SQLServerConnection


class DatabaseFactory:
    _registry: ClassVar[dict[str, Type[Database]]] = {
        "sqlserver": SQLServerConnection,
    }

    @classmethod
    def build(cls, engine: str, **kwargs: Any) -> Database:
        key = engine.lower().strip()
        conn_cls = cls._registry.get(key)
        if conn_cls is None:
            supported = ", ".join(sorted(cls._registry))
            raise ValueError(
                f"Unsupported engine {engine!r}. Supported engines: {supported}"
            )
        return conn_cls(**kwargs)
