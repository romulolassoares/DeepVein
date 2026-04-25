import logging
import os
from pathlib import Path

_LOGGER_IS_CONFIGURED = False
_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DEFAULT_LOG_LEVEL = "INFO"


def _resolve_level(level_name: str) -> int:
    level = getattr(logging, level_name.upper(), None)
    return level if isinstance(level, int) else logging.INFO


def setup_logging(
    level: str | None = None,
    log_file: str | None = None,
    log_format: str | None = None,
) -> None:
    global _LOGGER_IS_CONFIGURED
    if _LOGGER_IS_CONFIGURED:
        return

    log_level = _resolve_level(level or _DEFAULT_LOG_LEVEL)
    log_format = log_format or _DEFAULT_FORMAT
    log_file = log_file or os.getenv("DEEPVEIN_LOG_FILE")

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding="utf-8"))

    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
