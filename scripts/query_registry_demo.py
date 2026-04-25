from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.query_registry.models import Query
from src.query_registry.registry import QueryRegistry
from src.utils import get_logger, setup_logging

logger = get_logger(__name__)


def main() -> None:
    setup_logging()
    logger.info("Starting query registry demo")
    # Using in-memory DB keeps the example generic and self-contained.
    with QueryRegistry("test.db") as registry:
        logger.info("Registry initialized")
        sales_query = Query(
            id="generic-sales-report",
            sql="SELECT * FROM sales WHERE region = '$region' LIMIT $limit",
            groups=["reporting", "sales"],
            params={"region": "NA", "limit": "100"},
        )
        registry.add(sales_query)
        logger.info("Added query '%s'", sales_query.id)

        inventory_query = Query(
            id="generic-inventory-check",
            sql="SELECT sku, quantity FROM inventory WHERE warehouse = '$warehouse'",
            groups=["reporting", "inventory"],
            params={"warehouse": "WH-01"},
        )
        registry.add(inventory_query)
        logger.info("Added query '%s'", inventory_query.id)

        loaded = registry.get("generic-sales-report")
        logger.info("Loaded query '%s'", loaded.id)
        print("Loaded query:", loaded.to_dict())
        print("Rendered SQL:", loaded.render())

        loaded.params["limit"] = "10"
        registry.update(loaded)
        logger.info("Updated query '%s' params", loaded.id)
        print("Updated SQL:", registry.get("generic-sales-report").render())

        reporting_queries = registry.get_by_group("reporting")
        logger.info("Fetched %s reporting queries", len(reporting_queries))
        print("Queries in reporting group:", [q.id for q in reporting_queries])
    logger.info("Query registry demo finished")



if __name__ == "__main__":
    main()
