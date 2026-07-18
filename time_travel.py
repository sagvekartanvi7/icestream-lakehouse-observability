from pyiceberg.catalog.sql import SqlCatalog
import os

catalog = SqlCatalog(
    "icestream_catalog",
    **{
        "uri": "sqlite:///lakehouse/catalog.db",
        "warehouse": "file://" + os.path.abspath("lakehouse/warehouse"),
    },
)

table = catalog.load_table("orders.checkout_orders")

# List every snapshot (checkpoint) ever created
snapshots = list(table.history())

print(f"This table has {len(snapshots)} snapshots (checkpoints) saved:\n")
for i, snap in enumerate(snapshots):
    print(f"  [{i}] snapshot_id={snap.snapshot_id}")

print("\n--- Current data (latest snapshot) ---")
current_df = table.scan().to_pandas()
print(f"Total rows right now: {len(current_df)}")
print(current_df.tail(5))

if len(snapshots) > 1:
    # Go back in time to the FIRST snapshot ever created
    first_snapshot_id = snapshots[0].snapshot_id
    print(f"\n--- Time travel: data as it looked at snapshot [0] ---")
    old_df = table.scan(snapshot_id=first_snapshot_id).to_pandas()
    print(f"Total rows back then: {len(old_df)}")
    print(old_df.tail(5))
else:
    print("\nOnly one snapshot exists so far - run write_to_lakehouse.py longer to create more!")