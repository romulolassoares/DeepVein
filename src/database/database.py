from weakref import finalize
import pyarrow as pa
import pyarrow.parquet as pq

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Mapping, Optional
from pathlib import Path
from sqlalchemy import Engine, text
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils import config, get_logger

_QUERY_SNIPPET_MAX = 200
logger = get_logger(__name__)


class DatabaseConfigurationError(RuntimeError):
    pass


class DatabaseQueryError(RuntimeError):
    pass


class DatabaseExportError(RuntimeError):
    pass


def _get_database_config() -> Dict[str, Any]:
    raw = config.get("database")
    if raw is None:
        logger.exception('Missing "database" key in configuration')
        raise DatabaseConfigurationError(
            'Config must define a "database" key (see config/config.yml.example).'
        )
    logger.debug("Database configuration loaded")
    return raw


def _query_snippet(query: str):
    snippet = str(query).strip().replace("\n", " ")
    if len(snippet) > _QUERY_SNIPPET_MAX:
        snippet = snippet[:_QUERY_SNIPPET_MAX] + "..."
    return snippet


class Database(ABC):
    def __init__(self, database: str) -> None:
        self.config: Dict[str, Any] = _get_database_config()
        self.connection_url = self._build_url(database)
        self._engine: Optional[Engine] = None
        logger.info("Database adapter initialized for database '%s'", database)


    @abstractmethod
    def _build_url(self, database: str) -> str: ...


    def _create_engine(self) -> Engine:
        logger.debug("Creating SQLAlchemy engine")
        return create_engine(self.connection_url)


    def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        engine = self._create_engine()
        logger.info("Executing query")

        try:
            with engine.connect() as conn:
                query = text(query)
                params = params or {}

                result = conn.execute(query, params)

                rows = result.fetchall()
                keys = result.keys()

                conn.commit()
                logger.debug("Query executed and committed")
        except SQLAlchemyError as e:
            snippet = _query_snippet(query)
            logger.exception("Query execution failed")
            raise DatabaseQueryError(
                f"Query failed [sql: {snippet}]"
            ) from e
        finally:
            engine.dispose()
            logger.debug("SQLAlchemy engine disposed after execute")

        return [dict(zip(keys, row)) for row in rows]


    def execute_stream(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        logger.info("Executing streaming query into memory")
        rows: List[Dict[str, Any]] = []
        for batch in self._stream(query, params=params, chunk_size=chunk_size):
            rows.extend(batch.to_pylist())
        logger.info("Streaming query completed with %s rows", len(rows))
        return rows


    def _stream(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> Iterator[pa.RecordBatch]:
        engine = self._create_engine()
        schema = None
        logger.info("Starting query stream with chunk_size=%s", chunk_size)

        try:
            with engine.connect() as conn:
                query = text(query).execution_options(stream_results=True)
                params = params or {}

                try:
                    result = conn.execute(query, params)
                    keys = list(result.keys())

                    while True:
                        rows = result.fetchmany(chunk_size)

                        if not rows:
                            break

                        columns = {
                            key: [row[i] for row in rows]
                            for i, key in enumerate(keys)
                        }

                        try:
                            batch = pa.RecordBatch.from_pydict(columns)
                        except pa.ArrowInvalid as e:
                            raise DatabaseQueryError(
                                "Failed to convert query rows to Arrow format"
                            ) from e
                        
                        if schema is None:
                            schema = batch.schema

                        yield batch

                    conn.commit()
                    logger.debug("Streaming query committed")
                except SQLAlchemyError as e:
                    snippet = _query_snippet(query)
                    logger.exception("Streaming query execution failed")
                    raise DatabaseQueryError(
                        f"Query failed [sql: {snippet}]"
                    ) from e
        finally:
            engine.dispose()
            logger.debug("SQLAlchemy engine disposed after stream")


    def _extract_to_parquet_buffer(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        output: str = 'parquet_file',
    ) -> Path:
        path = Path(output)
        if path.suffix.lower() != ".parquet":
            path = path.with_suffix(".parquet")
        logger.info("Exporting query result to parquet buffer at '%s'", path)

        rows = self.execute(query, params)

        try:
            table = pa.Table.from_pylist(rows)
        except pa.ArrowInvalid as e:
            raise DatabaseExportError(
                "Failed to convert query rows to Arrow table for Parquet export"
            ) from e

        try:
            pq.write_table(table, path)
        except OSError as e:
            logger.exception("Failed to write parquet file '%s'", path)
            raise DatabaseExportError(
                f"Failed to write Parquet file to {path}"
            ) from e

        logger.info("Parquet export completed at '%s'", path)
        return path


    def _extract_to_parquet_stream(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        output: str = 'parquet_file',
        chunk_size: int = 1000,
    ) -> Path:
        path = Path(output)
        writer = None

        if path.suffix.lower() != ".parquet":
            path = path.with_suffix(".parquet")
        logger.info("Exporting query result to parquet stream at '%s'", path)

        try:
            for batch in self._stream(query, params=params, chunk_size=chunk_size):
                if writer is None:
                    try:
                        writer = pq.ParquetWriter(
                            path,
                            schema=batch.schema,
                            compression="snappy",
                        )
                    except (OSError, pa.ArrowInvalid) as e:
                        logger.exception("Failed to open parquet writer for '%s'", path)
                        raise DatabaseExportError(
                            f"Failed to open Parquet writer for {path}"
                        ) from e
                try:
                    writer.write_batch(batch)
                except (OSError, pa.ArrowInvalid) as e:
                    logger.exception("Failed while writing parquet rows to '%s'", path)
                    raise DatabaseExportError(
                        f"Failed while writing Parquet rows to {path}"
                    ) from e
        finally:
            if writer is not None:
                writer.close()
                logger.debug("Parquet writer closed for '%s'", path)

        logger.info("Parquet stream export completed at '%s'", path)
        return path


    def extract_to_parquet(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        output: str = 'parquet_file',
        chunk_size: int = 1000,
        stream: bool = False,
    ) -> Path:
        logger.info("Starting extract_to_parquet with stream=%s", stream)
        if stream:
            return self._extract_to_parquet_stream(
                query, params, output, chunk_size
            )
        return self._extract_to_parquet_buffer(query, params, output)