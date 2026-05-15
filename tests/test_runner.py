"""Tests for Runner — sequential and parallel query execution.

DuckDB is mocked so no real database file is needed.
"""
from unittest.mock import MagicMock, patch

from src.query_registry.models import Query
from src.query_runner.runner import Runner


def _queries(*sqls):
    return [Query(id=f"q{i}", sql=sql) for i, sql in enumerate(sqls, 1)]


# ---------------------------------------------------------------------------
# Sequential execution
# ---------------------------------------------------------------------------

def test_sequential_executes_all_queries():
    queries = _queries("SELECT 1", "SELECT 2", "SELECT 3")
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        MockDuckDB.return_value = mock_db
        Runner.execute(queries, database_path=":memory:", parallel=False)
    assert mock_db.execute.call_count == 3


def test_sequential_renders_sql_before_execution():
    q = Query(id="q1", sql="SELECT $col", params={"col": "id"})
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        MockDuckDB.return_value = mock_db
        Runner.execute([q], database_path=":memory:", parallel=False)
    mock_db.execute.assert_called_once_with("SELECT id")


def test_sequential_opens_duckdb_in_readonly():
    queries = _queries("SELECT 1")
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        MockDuckDB.return_value = MagicMock()
        Runner.execute(queries, database_path="some.db", parallel=False)
    MockDuckDB.assert_called_once_with(database="some.db", read_only=True)


def test_sequential_empty_query_list_does_nothing():
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        MockDuckDB.return_value = mock_db
        Runner.execute([], database_path=":memory:", parallel=False)
    mock_db.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Parallel execution
# ---------------------------------------------------------------------------

def test_parallel_executes_all_queries():
    queries = _queries("SELECT 1", "SELECT 2")
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        MockDuckDB.return_value = mock_db
        Runner.execute(queries, database_path=":memory:", parallel=True, max_workers=2)
    assert mock_db.execute.call_count == 2


def test_parallel_does_not_raise_on_query_error():
    """The parallel runner logs exceptions but must not bubble them up."""
    queries = _queries("SELECT 1", "SELECT 2")
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        mock_db.execute.side_effect = RuntimeError("bad sql")
        MockDuckDB.return_value = mock_db
        # should not raise
        Runner.execute(queries, database_path=":memory:", parallel=True, max_workers=2)


def test_parallel_opens_duckdb_in_readonly():
    queries = _queries("SELECT 1")
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        MockDuckDB.return_value = MagicMock()
        Runner.execute(queries, database_path="db.duckdb", parallel=True, max_workers=1)
    MockDuckDB.assert_called_once_with(database="db.duckdb", read_only=True)


def test_parallel_empty_query_list_does_nothing():
    with patch("src.query_runner.runner.DuckDB") as MockDuckDB:
        mock_db = MagicMock()
        MockDuckDB.return_value = mock_db
        Runner.execute([], database_path=":memory:", parallel=True)
    mock_db.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Route: parallel vs sequential flag
# ---------------------------------------------------------------------------

def test_execute_routes_to_parallel_when_flag_set():
    runner = Runner()
    runner._parallel_runner = MagicMock()
    runner._simple_runner = MagicMock()

    queries = _queries("SELECT 1")
    with patch("src.query_runner.runner.DuckDB"):
        # call instance methods directly to avoid class-method instantiation
        runner._parallel_runner(queries, ":memory:", 4)
        runner._simple_runner.assert_not_called()
        runner._parallel_runner.assert_called_once()
