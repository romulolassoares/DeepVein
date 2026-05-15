"""Tests for utility helpers: file type detection."""
from src.utils.check_file_type import get_file_type, is_parquet_file, is_csv_file


# ---------------------------------------------------------------------------
# get_file_type
# ---------------------------------------------------------------------------

def test_get_file_type_parquet():
    assert get_file_type("data.parquet") == "parquet"


def test_get_file_type_csv():
    assert get_file_type("data.csv") == "csv"


def test_get_file_type_uppercase_normalised():
    assert get_file_type("DATA.PARQUET") == "parquet"
    assert get_file_type("DATA.CSV") == "csv"


def test_get_file_type_no_extension():
    assert get_file_type("noextension") == "unknown"


def test_get_file_type_hidden_file_no_extension():
    # ".gitignore" has no extension — stem is empty, ext is ".gitignore"
    # os.path.splitext(".gitignore") → (".gitignore", "")
    assert get_file_type(".gitignore") == "unknown"


def test_get_file_type_full_path():
    assert get_file_type("/some/path/to/archive.parquet") == "parquet"


def test_get_file_type_relative_path():
    assert get_file_type("../data/records.csv") == "csv"


# ---------------------------------------------------------------------------
# is_parquet_file
# ---------------------------------------------------------------------------

def test_is_parquet_file_true():
    assert is_parquet_file("export.parquet") is True


def test_is_parquet_file_false_for_csv():
    assert is_parquet_file("export.csv") is False


def test_is_parquet_file_false_for_no_extension():
    assert is_parquet_file("export") is False


def test_is_parquet_file_case_insensitive():
    assert is_parquet_file("EXPORT.PARQUET") is True


# ---------------------------------------------------------------------------
# is_csv_file
# ---------------------------------------------------------------------------

def test_is_csv_file_true():
    assert is_csv_file("data.csv") is True


def test_is_csv_file_false_for_parquet():
    assert is_csv_file("data.parquet") is False


def test_is_csv_file_false_for_no_extension():
    assert is_csv_file("data") is False


def test_is_csv_file_case_insensitive():
    assert is_csv_file("DATA.CSV") is True
