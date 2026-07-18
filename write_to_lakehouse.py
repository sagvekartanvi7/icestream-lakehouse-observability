import json
import time
from kafka import KafkaConsumer
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa
import os
from pyiceberg.io.pyarrow import schema_to_pyarrow

catalog = SqlCatalog(
    "icestream_catalog",
    **{
        "uri": "sqlite:///lakehouse/catalog.db",
        "warehouse": "file://" + os.path.abspath("lakehouse/warehouse"),
    },
)

table = catalog.load_table("orders.checkout_orders")

consumer = KafkaConsumer(
    "checkout-orders",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

BATCH_SIZE = 10
buffer = []


def flush_to_iceberg(orders):
    """Writes a batch of orders permanently into the Iceberg table."""
    arrow_table = pa.Table.from_pylist(orders, schema=schema_to_pyarrow(table.schema()))
    table.append(arrow_table)
    print(f"Wrote {len(orders)} orders to Iceberg. (New snapshot created)\n")


if __name__ == "__main__":
    print("Writing orders from Kafka into the Iceberg lakehouse...")
    print(f"Batching {BATCH_SIZE} orders at a time. (Press Ctrl+C to stop)\n")

    for message in consumer:
        order = message.value

        # Iceberg needs every field filled in - replace missing tax_amount with 0.0
        # (in a real system we'd quarantine these separately, like our circuit breaker does)
        if order["tax_amount"] is None:
            order["tax_amount"] = 0.0

        buffer.append(order)
        print(f"Buffered: {order['order_id'][:8]}... ({len(buffer)}/{BATCH_SIZE})")

        if len(buffer) >= BATCH_SIZE:
            flush_to_iceberg(buffer)
            buffer = []