import os
from urllib.parse import quote_plus

from .database import Database
from src.utils import get_logger

DEFAULT_DRIVER_ENV_VAR = "DEFAULT_DRIVER"
_FALLBACK_DRIVER = "ODBC Driver 17 for SQL Server"
logger = get_logger(__name__)

def _try_load_dot_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        logger.debug("python-dotenv not installed; skipping .env load")
        return
    load_dotenv()
    logger.debug(".env loaded for SQL Server configuration")

_try_load_dot_env()

_default_driver = os.environ.get(DEFAULT_DRIVER_ENV_VAR, _FALLBACK_DRIVER)


class SQLServerConnection(Database):
    def _build_url(self, database: str) -> str:
        logger.info("Building SQL Server connection URL for database '%s'", database)
        cfg = self.config
        driver = quote_plus(cfg.get("driver", _default_driver))
        server = cfg.get("server", "localhost")
        port = cfg.get("port", 1433)
        trusted = cfg.get("trusted_connection", False)
        username = quote_plus(cfg.get("username", "sa"))
        password = quote_plus(cfg.get("password", "1234"))
        app_name = quote_plus(cfg.get("app_name", "py_db"))

        if trusted:
            logger.debug("Using trusted SQL Server connection")
            return (
                f"mssql+pyodbc://{server}:{port}/{database}"
                f"?driver={driver}"
                f"&trusted_connection=yes"
                f"&APP={app_name}"
            )
        if username and password:
            logger.debug("Using SQL Server username/password connection")
            return (
                f"mssql+pyodbc://{username}:{password}"
                f"@{server}:{port}/{database}"
                f"?driver={driver}"
                f"&trusted_connection=no"
                f"&APP={app_name}"
            )
        logger.exception("Invalid SQL Server database configuration")
        raise ValueError("Invalid database configuration")
