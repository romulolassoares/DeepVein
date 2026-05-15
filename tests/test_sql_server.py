import pytest
from unittest.mock import patch
from urllib.parse import quote_plus

from src.database.sql_server import SQLServerConnection


def _make_conn(cfg):
    """Instantiate SQLServerConnection with a fake config; no real DB needed."""
    with patch("src.database.database._get_database_config", return_value=cfg):
        return SQLServerConnection(database="mydb")


_BASE_CFG = {
    "server": "host",
    "port": 1433,
    "driver": "ODBC Driver 17 for SQL Server",
    "trusted_connection": False,
    "username": "user",
    "password": "pass",
}


def test_url_with_credentials():
    conn = _make_conn(_BASE_CFG)
    url = conn.connection_url
    assert quote_plus("user") in url
    assert quote_plus("pass") in url
    assert "host:1433/mydb" in url
    assert "trusted_connection=no" in url


def test_url_with_trusted_connection():
    cfg = {**_BASE_CFG, "trusted_connection": True}
    conn = _make_conn(cfg)
    url = conn.connection_url
    assert "trusted_connection=yes" in url
    # no user:password@ segment in trusted mode
    assert "@" not in url


def test_url_includes_driver():
    conn = _make_conn(_BASE_CFG)
    assert quote_plus("ODBC Driver 17 for SQL Server") in conn.connection_url


def test_url_includes_default_app_name():
    conn = _make_conn(_BASE_CFG)
    assert "APP=" in conn.connection_url


def test_url_custom_app_name():
    cfg = {**_BASE_CFG, "app_name": "myapp"}
    conn = _make_conn(cfg)
    assert quote_plus("myapp") in conn.connection_url


def test_url_raises_without_credentials_and_not_trusted():
    cfg = {
        "server": "host",
        "port": 1433,
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": False,
        "username": "",
        "password": "",
    }
    with pytest.raises(ValueError, match="Invalid database configuration"):
        _make_conn(cfg)
