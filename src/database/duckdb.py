import re
from collections.abc import Callable, Sequence
from typing import Any, List, Optional
from src.utils import is_parquet_file, is_csv_file
from src.udf import udf_loader
from src.utils import get_logger

import duckdb
import polars as pl

logger = get_logger(__name__)


class DuckDB:
    def __init__(
        self,
        database: Optional[str] = None,
        read_only: bool = False,
    ) -> None:
        database = database or ":memory:"
        logger.info("Connecting DuckDB database '%s' (read_only=%s)", database, read_only)
        self.connection = duckdb.connect(database, read_only=read_only)
        udf_loader("functions", self)
        logger.info("DuckDB initialized and UDFs loaded")
        

    def _insert_csv_data(self, table_name: str, filename: str) -> None:
        logger.info("Inserting CSV '%s' into table '%s'", filename, table_name)
        self.connection.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv('{filename}', header=true, all_varchar=true)
        """)
    

    def _insert_parquet_data(self, table_name: str, filename: str) -> None:
        logger.info("Inserting parquet '%s' into table '%s'", filename, table_name)
        self.connection.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{filename}')
        """)
        

    def _insert_dataframe(self, table_name: str, df: pl.DataFrame) -> None:
        logger.info("Inserting DataFrame into table '%s'", table_name)
        temp = "__name_forge_df__"
        self.connection.register(temp, df)
        self.connection.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM {temp};
        """)
    

    def insert_data(self, table_name: str, data: object) -> None:
        logger.info("Inserting data into table '%s'", table_name)
        if isinstance(data, str) and is_csv_file(data):
            self._insert_csv_data(table_name, data)
        elif isinstance(data, str) and is_parquet_file(data):
            self._insert_parquet_data(table_name, data)
        elif isinstance(data, pl.DataFrame):
            self._insert_dataframe(table_name, data)
        else:
            logger.exception("Unsupported data type for insert_data: %s", type(data).__name__)
            raise TypeError(
                "data must be str (CSV or Parquet) or polars.DataFrame; "
                f"got {type(data).__name__}."
            )


    def _check_if_exists(self, query: str) -> bool:
        logger.debug("Checking existence with metadata query")
        result = self.connection.execute(query).pl()
        return not result.is_empty()


    def function_exists(self, function_name: str) -> bool:
        logger.debug("Checking if function '%s' exists", function_name)
        query = f"""
            SELECT 1
            FROM duckdb_functions()
            WHERE lower(function_type) = 'scalar'
            and function_name = '{function_name}';
        """
        return self._check_if_exists(query)


    def register_function(self, func_name: str, func_impl: callable) -> None:
        logger.info("Registering DuckDB function '%s'", func_name)
        try:
            self.connection.remove_function(func_name)
        except duckdb.InvalidInputException:
            pass
        try:
            self.connection.create_function(func_name, func_impl)
        except duckdb.CatalogException as e:
            if "already exists" not in str(e).lower():
                raise
        except duckdb.NotImplementedException as e:
            if "already created" not in str(e).lower():
                raise


    def table_exists(self, table_name: str) -> bool:
        logger.debug("Checking if table '%s' exists", table_name)
        query = f"""
            SELECT 1
            FROM duckdb_tables()
            WHERE TABLE_NAME = '{table_name}';
        """
        return self._check_if_exists(query)

    
    def execute(self, query: str):
        logger.debug("Executing DuckDB SQL statement")
        return self.connection.execute(query)

    
    def get_columns(self, table_name: str) -> List[str]:
        logger.info("Fetching columns from table '%s'", table_name)
        query = f"DESCRIBE {table_name}"
        result = self.connection.execute(query).fetchall()
        result = [row[0] for row in result]
        return result