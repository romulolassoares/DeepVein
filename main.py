from src.database.database_factory import DatabaseFactory
from src.utils import get_logger, setup_logging

logger = get_logger(__name__)

def main():
    setup_logging()
    logger.info("Application started")
    logger.info("Building database connection for sqlserver engine")

    kwargs = {"database": "rlass"}
    db = DatabaseFactory.build(engine="sqlserver", **kwargs)
    logger.info("Database connection built successfully")

    query = "SELECT * from tb_test_script"
    logger.info("Starting query extraction")

    result = db.extract_to_parquet(query, output="script_test")

    logger.info("Query extraction completed")
    print(result)

if __name__ == "__main__":
    main()
