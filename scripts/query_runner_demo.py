from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from src.database import DuckDB
from src.query_registry import Query
from src.query_runner import Runner
from src.utils import get_logger, setup_logging

logger = get_logger(__name__)


def build_demo_database(database_path: str) -> None:
    logger.info("Building demo database at '%s'", database_path)
    db = DuckDB(database=database_path, read_only=False)

    customers = pl.DataFrame(
        {
            "customer_id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dan"],
            "region": ["north", "south", "north", "west"],
        }
    )

    orders = pl.DataFrame(
        {
            "order_id": [101, 102, 103, 104, 105, 106],
            "customer_id": [1, 1, 2, 3, 3, 4],
            "amount": [120.0, 75.0, 50.0, 40.0, 160.0, 90.0],
            "status": ["paid", "pending", "paid", "paid", "paid", "pending"],
        }
    )

    db.insert_data("customers", customers)
    db.insert_data("orders", orders)
    logger.info("Inserted demo tables: customers and orders")


def build_demo_queries() -> list[Query]:
    logger.info("Building demo query list")
    return [
        Query(
            id="q01_paid_orders_total",
            sql="""
                SELECT count(*) AS paid_orders
                FROM orders
                WHERE status = 'paid'
            """,
        ),
        Query(
            id="q02_top_customers_by_spend",
            sql="""
                SELECT
                    c.customer_id,
                    c.name,
                    SUM(o.amount) AS total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name
                ORDER BY total_spent DESC
                LIMIT 3
            """,
        ),
        Query(
            id="q03_region_revenue",
            sql="""
                SELECT
                    c.region,
                    ROUND(SUM(o.amount), 2) AS revenue
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.status = 'paid'
                GROUP BY c.region
                ORDER BY revenue DESC
            """,
        ),
    ]


def print_query_results(database_path: str, queries: list[Query]) -> None:
    logger.info("Printing query results from '%s'", database_path)
    db = DuckDB(database=database_path, read_only=True)
    for query in queries:
        logger.info("Printing result for query '%s'", query.id)
        print(f"\n--- {query.id} ---")
        print(db.execute(query.render()).pl())


def main() -> None:
    setup_logging()
    logger.info("Starting query runner demo")
    database_path = str(Path(__file__).with_name("query_runner_demo.duckdb"))
    queries = build_demo_queries()

    build_demo_database(database_path)

    Runner.execute(
        queries=queries,
        database_path=database_path,
        parallel=True,
        max_workers=4,
    )
    logger.info("Runner execution completed")

    print_query_results(database_path, queries)
    print(f"\nDemo database created at: {database_path}")
    logger.info("Query runner demo finished")


if __name__ == "__main__":
    main()
