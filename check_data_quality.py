from kafka import KafkaConsumer
import json

TOPIC_NAME = "checkout-orders"

# Connect as a "reader" of the Kafka conveyor belt
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",  # only look at NEW messages from now on
)


def check_order(order):
    """
    Our first data quality rule:
    'tax_amount must never be empty (None)'
    Returns True if the order PASSES, False if it FAILS.
    """
    if order["tax_amount"] is None:
        return False
    return True


if __name__ == "__main__":
    print("Watching for orders... (Press Ctrl+C to stop)\n")

    good_count = 0
    bad_count = 0

    for message in consumer:
        order = message.value
        is_good = check_order(order)

        if is_good:
            good_count += 1
            print(f"✅ GOOD | order_id={order['order_id'][:8]}... tax_amount={order['tax_amount']}")
        else:
            bad_count += 1
            print(f"❌ BAD  | order_id={order['order_id'][:8]}... tax_amount=MISSING!")

        print(f"   (Totals so far -> good: {good_count}, bad: {bad_count})\n")