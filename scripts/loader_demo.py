from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from src.loader import Loader
from src.database import DuckDB
from src.utils import get_logger, setup_logging

logger = get_logger(__name__)


def main() -> None:
    setup_logging()
    logger.info("Starting loader demo")

    logger.info("Initializing Loader | engine=sqlserver | database=rlass | duckdb_path=database.duckdb")
    loader = Loader(
        engine="sqlserver",
        database="rlass",
        duckdb_path="database.duckdb"
    )

    table = "tb_test_script"
    output = "storage/tmp/file.parquet"
    chunk_size = 10000

    logger.info("Loading table into DuckDB | table=%s | output=%s | chunk_size=%d | stream=True", table, output, chunk_size)
    loader.load(
        table=table,
        output=output,
        chunk_size=chunk_size,
        stream=True,
    )
    logger.info("Table loaded successfully | table=%s | output=%s", table, output)

    logger.info("Querying DuckDB | query=SELECT * FROM %s", table)
    ddb = DuckDB(database="database.duckdb")
    res = ddb.execute(f"SELECT * FROM {table}")
    logger.info("Query completed | table=%s", table)
    print(res.pl())
    
if __name__ == "__main__":
    main()