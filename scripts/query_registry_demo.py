from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.query_registry.models import Query
from src.query_registry.registry import QueryRegistry


def main() -> None:
    # Using in-memory DB keeps the example generic and self-contained.
    with QueryRegistry("test.db") as registry:
        sales_query = Query(
            id="generic-sales-report",
            sql="SELECT * FROM sales WHERE region = '$region' LIMIT $limit",
            groups=["reporting", "sales"],
            params={"region": "NA", "limit": "100"},
        )
        registry.add(sales_query)

        inventory_query = Query(
            id="generic-inventory-check",
            sql="SELECT sku, quantity FROM inventory WHERE warehouse = '$warehouse'",
            groups=["reporting", "inventory"],
            params={"warehouse": "WH-01"},
        )
        registry.add(inventory_query)

        loaded = registry.get("generic-sales-report")
        print("Loaded query:", loaded.to_dict())
        print("Rendered SQL:", loaded.render())

        loaded.params["limit"] = "10"
        registry.update(loaded)
        print("Updated SQL:", registry.get("generic-sales-report").render())

        reporting_queries = registry.get_by_group("reporting")
        print("Queries in reporting group:", [q.id for q in reporting_queries])



if __name__ == "__main__":
    main()
