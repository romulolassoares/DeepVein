"""Tests for the DuckDB adapter.

udf_loader is mocked in every test so the real functions/ directory and
dynamic import machinery are not involved.
"""
import pytest
import polars as pl
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Fixture: in-memory DuckDB with UDF loading disabled
# ---------------------------------------------------------------------------

@pytest.fixture
def ddb():
    """Return a DuckDB instance backed by :memory: with UDFs suppressed."""
    with patch("src.database.duckdb.udf_loader"):
        from src.database.duckdb import DuckDB
        return DuckDB(database=None)  # None → :memory:


# ---------------------------------------------------------------------------
# table_exists
# ---------------------------------------------------------------------------

def test_table_exists_false_for_new_db(ddb):
    assert ddb.table_exists("nonexistent") is False


def test_table_exists_true_after_create(ddb):
    ddb.connection.execute("CREATE TABLE t (id INTEGER)")
    assert ddb.table_exists("t") is True


# ---------------------------------------------------------------------------
# function_exists
# ---------------------------------------------------------------------------

def test_function_exists_false_for_unknown(ddb):
    assert ddb.function_exists("does_not_exist") is False


def test_function_exists_true_after_register(ddb):
    ddb.register_function("mysum", lambda x, y: x + y)
    assert ddb.function_exists("mysum") is True


# ---------------------------------------------------------------------------
# register_function
# ---------------------------------------------------------------------------

def test_register_function_can_be_called_in_sql(ddb):
    ddb.register_function("triple", lambda x: x * 3)
    result = ddb.execute("SELECT triple(4)").fetchone()[0]
    assert result == 12


def test_register_function_replaces_existing(ddb):
    ddb.register_function("greet", lambda x: f"hello {x}")
    ddb.register_function("greet", lambda x: f"hi {x}")  # re-register, should not raise
    result = ddb.execute("SELECT greet('world')").fetchone()[0]
    assert result == "hi world"


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------

def test_execute_returns_relation(ddb):
    rel = ddb.execute("SELECT 42 AS n")
    assert rel.fetchone()[0] == 42


# ---------------------------------------------------------------------------
# get_columns
# ---------------------------------------------------------------------------

def test_get_columns(ddb):
    ddb.connection.execute("CREATE TABLE emp (id INTEGER, name VARCHAR)")
    cols = ddb.get_columns("emp")
    assert cols == ["id", "name"]


# ---------------------------------------------------------------------------
# insert_data — CSV
# ---------------------------------------------------------------------------

def test_insert_data_csv(ddb, tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
    ddb.insert_data("people", str(csv_file))
    rows = ddb.connection.execute("SELECT COUNT(*) FROM people").fetchone()[0]
    assert rows == 2


# ---------------------------------------------------------------------------
# insert_data — Parquet
# ---------------------------------------------------------------------------

def test_insert_data_parquet(ddb, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    parquet_file = tmp_path / "data.parquet"
    pq.write_table(table, parquet_file)

    ddb.insert_data("records", str(parquet_file))
    rows = ddb.connection.execute("SELECT COUNT(*) FROM records").fetchone()[0]
    assert rows == 3


# ---------------------------------------------------------------------------
# insert_data — Polars DataFrame
# ---------------------------------------------------------------------------

def test_insert_data_dataframe(ddb):
    df = pl.DataFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    ddb.insert_data("df_table", df)
    rows = ddb.connection.execute("SELECT COUNT(*) FROM df_table").fetchone()[0]
    assert rows == 3


def test_insert_data_dataframe_column_values(ddb):
    df = pl.DataFrame({"n": [7]})
    ddb.insert_data("single", df)
    val = ddb.connection.execute("SELECT n FROM single").fetchone()[0]
    assert val == 7


# ---------------------------------------------------------------------------
# insert_data — unsupported type
# ---------------------------------------------------------------------------

def test_insert_data_unsupported_type_raises(ddb):
    with pytest.raises(TypeError, match="data must be str"):
        ddb.insert_data("t", 42)


def test_insert_data_unsupported_string_extension_raises(ddb):
    with pytest.raises(TypeError):
        ddb.insert_data("t", "data.json")
