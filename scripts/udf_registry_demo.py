from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from src.udf.registry import udf_loader
from src.database import DuckDB
from src.utils import get_logger, setup_logging

logger = get_logger(__name__)

def main() -> None:
    setup_logging()
    logger.info("Starting UDF registry demo")
    database_path = str(Path(__file__).with_name("udf_registry_demo.duckdb"))
    functions_dir = PROJECT_ROOT / "functions"
    logger.info("Using functions directory '%s'", functions_dir)

    db = udf_loader(functions_dir, database_path)
    logger.info("UDF loader completed")

    items = pl.DataFrame(
        {
            "sku": ["A1", "B2", "C3", "D4"],
            "base_qty": [10, 25, 0, 100],
            "extra_qty": [3, 5, 7, 0],
            "note": ["in stock", "", "rush", None],
        }
    )
    db.insert_data("items", items)
    logger.info("Inserted demo items table")

    print("Registered UDFs from", functions_dir)
    print("\n--- plus(base_qty, extra_qty) ---")
    logger.info("Running plus() demo query")
    print(
        db.execute(
            """
            SELECT
                sku,
                plus(base_qty, extra_qty) AS total_qty
            FROM items
            ORDER BY sku
            """
        ).pl()
    )

    print("\n--- isnull(note, '(no note)') ---")
    logger.info("Running isnull() demo query")
    print(
        db.execute(
            """
            SELECT
                sku,
                COALESCE(
                    isnull(CAST(note AS VARCHAR), '(no note)'),
                    '(no note)'
                ) AS note_display
            FROM items
            ORDER BY sku
            """
        ).pl()
    )

    print(f"\nDatabase file: {database_path}")
    logger.info("UDF registry demo finished")


if __name__ == "__main__":
    main()
