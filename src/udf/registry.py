from __future__ import annotations

import importlib.util
import inspect
import types
from pathlib import Path
from typing import TYPE_CHECKING, List

from src.utils import get_logger

if TYPE_CHECKING:
    from src.database.duckdb import DuckDB

logger = get_logger(__name__)


def _load_module_from_file(file: Path) -> types.ModuleType:
    logger.debug("Loading UDF module from '%s'", file)
    name = file.stem
    spec = importlib.util.spec_from_file_location(name, file)
    if spec is None or spec.loader is None:
        logger.exception("Could not load module spec for '%s'", file)
        raise ImportError(f"Could not load module spec for {file}")
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        logger.exception("Failed to execute UDF module '%s'", file)
        raise RuntimeError(f"Failed to execute module {file}") from e
    return mod


def _get_functions(mod: types.ModuleType) -> List:
    logger.debug("Inspecting module '%s' for functions", mod.__name__)
    return inspect.getmembers(mod, inspect.isfunction)


def _get_python_files(path: Path) -> list[Path]:
    if not path.is_dir():
        logger.exception("Expected directory for UDF loader, got '%s'", path)
        raise NotADirectoryError(f"Expected a directory, got {path}")

    files = path.glob("*.py")

    return [file for file in files if not file.stem.startswith("_")]

def udf_loader(path: str | Path, ddb: DuckDB) -> None:
    root = Path(path)
    registered = {}
    logger.info("Loading UDFs from '%s'", root)

    py_files = _get_python_files(root)

    for file in py_files:
        try:
            mod = _load_module_from_file(file)
        except (ImportError, RuntimeError) as e:
            logger.exception("Failed to load UDF module from '%s'", file)
            raise RuntimeError(f"Failed to load UDF module from {file}") from e

        functions = _get_functions(mod)
        
        for name, obj in functions:
            if name in registered.keys():
                logger.exception("Duplicate UDF function detected: '%s'", name)
                raise ValueError(
                    f"The function {name!r} is already registered by {registered[name]}"
                )
            
            try:
                ddb.register_function(
                    func_name=name,
                    func_impl=obj,
                )
                registered.update({name: str(file)})
                logger.info("Registered UDF '%s' from '%s'", name, file)
            except Exception as e:
                logger.exception("Failed to register UDF '%s' from '%s'", name, file)
                raise RuntimeError(
                    f"Failed to register UDF {name!r} from {file}"
                ) from e