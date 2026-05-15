import pytest
from unittest.mock import patch

from src.database.database_factory import DatabaseFactory
from src.database.sql_server import SQLServerConnection


_FAKE_CFG = {
    "server": "host",
    "port": 1433,
    "driver": "ODBC Driver 17 for SQL Server",
    "trusted_connection": False,
    "username": "user",
    "password": "pass",
}


def test_build_sqlserver_returns_correct_type():
    with patch("src.database.database._get_database_config", return_value=_FAKE_CFG):
        conn = DatabaseFactory.build(engine="sqlserver", database="mydb")
    assert isinstance(conn, SQLServerConnection)


def test_build_engine_name_is_case_insensitive():
    with patch("src.database.database._get_database_config", return_value=_FAKE_CFG):
        conn = DatabaseFactory.build(engine="SQLServer", database="mydb")
    assert isinstance(conn, SQLServerConnection)


def test_build_engine_name_strips_whitespace():
    with patch("src.database.database._get_database_config", return_value=_FAKE_CFG):
        conn = DatabaseFactory.build(engine="  sqlserver  ", database="mydb")
    assert isinstance(conn, SQLServerConnection)


def test_build_unsupported_engine_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported engine"):
        DatabaseFactory.build(engine="mysql", database="mydb")


def test_build_unsupported_engine_lists_supported_engines():
    with pytest.raises(ValueError, match="sqlserver"):
        DatabaseFactory.build(engine="oracle", database="mydb")
