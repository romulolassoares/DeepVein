"""Tests for the abstract Database base class (execute, stream, parquet export).

SQLServerConnection is used as the concrete subclass; all network I/O is
mocked so no real database is needed.
"""
import pytest
import pyarrow as pa
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from sqlalchemy.exc import SQLAlchemyError

from src.database.database import (
    Database,
    DatabaseConfigurationError,
    DatabaseExportError,
    DatabaseQueryError,
    _query_snippet,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(cfg=None):
    """Build a SQLServerConnection with a mocked config and no real engine."""
    cfg = cfg or {
        "server": "host",
        "port": 1433,
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": False,
        "username": "u",
        "password": "p",
    }
    with patch("src.database.database._get_database_config", return_value=cfg):
        from src.database.sql_server import SQLServerConnection
        return SQLServerConnection(database="testdb")


def _mock_engine(rows, keys):
    """Return a mock SQLAlchemy engine that returns given rows and column keys."""
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_result.keys.return_value = keys

    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    return mock_engine


# ---------------------------------------------------------------------------
# _query_snippet
# ---------------------------------------------------------------------------

def test_query_snippet_short():
    assert _query_snippet("SELECT 1") == "SELECT 1"


def test_query_snippet_truncates_long():
    long_sql = "SELECT " + "x" * 300
    snippet = _query_snippet(long_sql)
    assert snippet.endswith("...")
    assert len(snippet) <= 203  # 200 chars + "..."


def test_query_snippet_strips_newlines():
    assert "\n" not in _query_snippet("SELECT\n1")


# ---------------------------------------------------------------------------
# _get_database_config
# ---------------------------------------------------------------------------

def test_get_database_config_missing_key_raises():
    from src.database.database import _get_database_config
    with patch("src.database.database.config", {}):
        with pytest.raises(DatabaseConfigurationError):
            _get_database_config()


# ---------------------------------------------------------------------------
# Database.execute
# ---------------------------------------------------------------------------

def test_execute_returns_list_of_dicts():
    db = _make_db()
    engine = _mock_engine(rows=[(1, "Alice"), (2, "Bob")], keys=["id", "name"])
    db._create_engine = lambda: engine

    result = db.execute("SELECT id, name FROM users")
    assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_execute_empty_result():
    db = _make_db()
    engine = _mock_engine(rows=[], keys=["id"])
    db._create_engine = lambda: engine

    assert db.execute("SELECT id FROM users WHERE 1=0") == []


def test_execute_raises_database_query_error_on_sqlalchemy_error():
    db = _make_db()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = SQLAlchemyError("connection refused")
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    db._create_engine = lambda: mock_engine

    with pytest.raises(DatabaseQueryError, match="Query failed"):
        db.execute("SELECT 1")


def test_execute_disposes_engine_on_success():
    db = _make_db()
    engine = _mock_engine(rows=[], keys=[])
    db._create_engine = lambda: engine
    db.execute("SELECT 1")
    engine.dispose.assert_called_once()


def test_execute_disposes_engine_on_error():
    db = _make_db()
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = SQLAlchemyError("boom")
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn
    db._create_engine = lambda: mock_engine

    with pytest.raises(DatabaseQueryError):
        db.execute("SELECT 1")

    mock_engine.dispose.assert_called_once()


# ---------------------------------------------------------------------------
# Database.execute_stream
# ---------------------------------------------------------------------------

def test_execute_stream_returns_all_rows():
    db = _make_db()
    batch1 = pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["Alice", "Bob"]})
    batch2 = pa.RecordBatch.from_pydict({"id": [3], "name": ["Carol"]})

    db._stream = MagicMock(return_value=iter([batch1, batch2]))

    result = db.execute_stream("SELECT 1")
    assert len(result) == 3
    assert result[0]["id"] == 1
    assert result[2]["name"] == "Carol"


# ---------------------------------------------------------------------------
# Database.extract_to_parquet
# ---------------------------------------------------------------------------

def test_extract_to_parquet_buffer_adds_extension(tmp_path):
    db = _make_db()
    db.execute = MagicMock(return_value=[{"x": 1}])
    output = str(tmp_path / "out")

    result = db.extract_to_parquet("SELECT 1", output=output, stream=False)

    assert result.suffix == ".parquet"
    assert result.exists()


def test_extract_to_parquet_buffer_keeps_extension(tmp_path):
    db = _make_db()
    db.execute = MagicMock(return_value=[{"x": 1}])
    output = str(tmp_path / "out.parquet")

    result = db.extract_to_parquet("SELECT 1", output=output, stream=False)
    assert result.suffix == ".parquet"


def test_extract_to_parquet_raises_on_arrow_error(tmp_path):
    db = _make_db()
    # Return a value that cannot be converted to an Arrow table
    db.execute = MagicMock(return_value=[{"bad": object()}])

    with pytest.raises(DatabaseExportError):
        db.extract_to_parquet("SELECT 1", output=str(tmp_path / "out"), stream=False)


def test_extract_to_parquet_stream_writes_file(tmp_path):
    db = _make_db()
    batch = pa.RecordBatch.from_pydict({"id": [1, 2]})
    db._stream = MagicMock(return_value=iter([batch]))
    output = str(tmp_path / "stream_out.parquet")

    result = db.extract_to_parquet("SELECT 1", output=output, stream=True)

    assert result.exists()
    assert result.suffix == ".parquet"


def test_extract_to_parquet_routes_stream_flag(tmp_path):
    db = _make_db()
    db._extract_to_parquet_buffer = MagicMock(return_value=Path(tmp_path / "a.parquet"))
    db._extract_to_parquet_stream = MagicMock(return_value=Path(tmp_path / "b.parquet"))

    db.extract_to_parquet("SELECT 1", output="x", stream=False)
    db._extract_to_parquet_buffer.assert_called_once()
    db._extract_to_parquet_stream.assert_not_called()

    db.extract_to_parquet("SELECT 1", output="x", stream=True)
    db._extract_to_parquet_stream.assert_called_once()
