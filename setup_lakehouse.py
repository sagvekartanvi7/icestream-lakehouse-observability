from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, DoubleType
)
import os

os.makedirs("lakehouse", exist_ok=True)

catalog = SqlCatalog(
    "icestream_catalog",
    **{
        "uri": "sqlite:///lakehouse/catalog.db",
        "warehouse": "file://" + os.path.abspath("lakehouse/warehouse"),
    },
)

if "orders" not in [n[0] for n in catalog.list_namespaces()]:
    catalog.create_namespace("orders")

schema = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer_name", StringType(), required=False),
    NestedField(3, "item", StringType(), required=False),
    NestedField(4, "price", DoubleType(), required=False),
    NestedField(5, "tax_amount", DoubleType(), required=False),
    NestedField(6, "country", StringType(), required=False),
)

table_name = "orders.checkout_orders"

try:
    catalog.load_table(table_name)
    print(f"Table already exists: {table_name}")
except Exception:
    catalog.create_table(table_name, schema=schema)
    print(f"Created Iceberg table: {table_name}")

print("\nLakehouse is ready. Location: ./lakehouse/warehouse")
