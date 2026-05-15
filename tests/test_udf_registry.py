"""Tests for the UDF registry — dynamic module loading and function registration."""
import textwrap
import pytest
from pathlib import Path
from unittest.mock import MagicMock, call

from src.udf.registry import (
    udf_loader,
    _get_python_files,
    _load_module_from_file,
    _get_functions,
)


# ---------------------------------------------------------------------------
# _get_python_files
# ---------------------------------------------------------------------------

def test_get_python_files_returns_py_files(tmp_path):
    (tmp_path / "funcs.py").write_text("def f(): pass")
    (tmp_path / "helpers.py").write_text("def g(): pass")
    files = _get_python_files(tmp_path)
    names = {f.name for f in files}
    assert names == {"funcs.py", "helpers.py"}


def test_get_python_files_excludes_underscore_prefixed(tmp_path):
    (tmp_path / "public.py").write_text("def f(): pass")
    (tmp_path / "_private.py").write_text("def g(): pass")
    (tmp_path / "__init__.py").write_text("")
    files = _get_python_files(tmp_path)
    names = {f.name for f in files}
    assert "public.py" in names
    assert "_private.py" not in names
    assert "__init__.py" not in names


def test_get_python_files_empty_directory(tmp_path):
    assert _get_python_files(tmp_path) == []


def test_get_python_files_raises_for_non_directory(tmp_path):
    f = tmp_path / "file.py"
    f.write_text("")
    with pytest.raises(NotADirectoryError):
        _get_python_files(f)


def test_get_python_files_raises_for_missing_path():
    with pytest.raises(NotADirectoryError):
        _get_python_files(Path("/nonexistent/path"))


# ---------------------------------------------------------------------------
# _load_module_from_file
# ---------------------------------------------------------------------------

def test_load_module_from_file_imports_correctly(tmp_path):
    src = tmp_path / "mymod.py"
    src.write_text("VALUE = 42\ndef answer(): return VALUE\n")
    mod = _load_module_from_file(src)
    assert mod.VALUE == 42
    assert mod.answer() == 42


def test_load_module_from_file_raises_on_syntax_error(tmp_path):
    src = tmp_path / "bad.py"
    src.write_text("def broken(: pass")
    with pytest.raises(RuntimeError, match="Failed to execute module"):
        _load_module_from_file(src)


# ---------------------------------------------------------------------------
# _get_functions
# ---------------------------------------------------------------------------

def test_get_functions_returns_only_functions(tmp_path):
    src = tmp_path / "mixed.py"
    src.write_text(textwrap.dedent("""
        CONSTANT = 1
        class MyClass: pass
        def my_func(x): return x
    """))
    mod = _load_module_from_file(src)
    funcs = _get_functions(mod)
    names = [name for name, _ in funcs]
    assert "my_func" in names
    assert "CONSTANT" not in names
    assert "MyClass" not in names


# ---------------------------------------------------------------------------
# udf_loader
# ---------------------------------------------------------------------------

def test_udf_loader_registers_all_functions(tmp_path):
    (tmp_path / "math.py").write_text(textwrap.dedent("""
        def add(x: int, y: int) -> int:
            return x + y
        def mul(x: int, y: int) -> int:
            return x * y
    """))
    mock_ddb = MagicMock()
    udf_loader(tmp_path, mock_ddb)
    registered = {c.kwargs["func_name"] for c in mock_ddb.register_function.call_args_list}
    assert registered == {"add", "mul"}


def test_udf_loader_skips_empty_directory(tmp_path):
    mock_ddb = MagicMock()
    udf_loader(tmp_path, mock_ddb)
    mock_ddb.register_function.assert_not_called()


def test_udf_loader_raises_on_duplicate_function_name(tmp_path):
    (tmp_path / "a.py").write_text("def dup(x): return x")
    (tmp_path / "b.py").write_text("def dup(x): return x * 2")
    mock_ddb = MagicMock()
    with pytest.raises(ValueError, match="already registered"):
        udf_loader(tmp_path, mock_ddb)


def test_udf_loader_raises_on_broken_module(tmp_path):
    (tmp_path / "broken.py").write_text("def bad(: pass")
    mock_ddb = MagicMock()
    with pytest.raises(RuntimeError, match="Failed to load UDF module"):
        udf_loader(tmp_path, mock_ddb)


def test_udf_loader_raises_on_non_directory():
    mock_ddb = MagicMock()
    with pytest.raises(NotADirectoryError):
        udf_loader(Path("/nonexistent/dir"), mock_ddb)


def test_udf_loader_accepts_string_path(tmp_path):
    (tmp_path / "fn.py").write_text("def hello() -> str: return 'hi'")
    mock_ddb = MagicMock()
    udf_loader(str(tmp_path), mock_ddb)  # str, not Path
    mock_ddb.register_function.assert_called_once()
