from kafka import KafkaConsumer, KafkaProducer
import json
from collections import deque
import csv
import os
from datetime import datetime

SOURCE_TOPIC = "checkout-orders"
DLQ_TOPIC = "checkout-orders-dlq"  # "Dead Letter Queue" - our quarantine lane

ERROR_THRESHOLD = 0.02  # 2% - if error rate crosses this, we "trip the breaker"
WINDOW_SIZE = 20        # look at the last 20 orders to calculate error rate

INCIDENT_LOG_FILE = "incident_log.csv"

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# A "sliding window" - remembers only the last WINDOW_SIZE results (True=good, False=bad)
recent_results = deque(maxlen=WINDOW_SIZE)

breaker_tripped = False


def check_order(order):
    return order["tax_amount"] is not None


def log_incident(event_type, error_rate):
    """Records a timestamped entry every time the breaker trips or resets."""
    file_exists = os.path.exists(INCIDENT_LOG_FILE)
    with open(INCIDENT_LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "event", "error_rate", "reason"])
        reason = (
            f"Error rate {error_rate:.1%} exceeded {ERROR_THRESHOLD:.0%} threshold"
            if event_type == "TRIPPED"
            else f"Error rate recovered to {error_rate:.1%}"
        )
        writer.writerow([datetime.now().isoformat(timespec="seconds"), event_type, f"{error_rate:.1%}", reason])


if __name__ == "__main__":
    print(f"Circuit breaker active. Threshold: {ERROR_THRESHOLD*100}% over last {WINDOW_SIZE} orders.\n")

    for message in consumer:
        order = message.value
        is_good = check_order(order)
        recent_results.append(is_good)

        # Calculate current error rate over the recent window
        bad_in_window = recent_results.count(False)
        error_rate = bad_in_window / len(recent_results)

        if is_good:
            print(f"✅ GOOD | order_id={order['order_id'][:8]}... | error_rate={error_rate:.1%}")
        else:
            # Bad order -> redirect to quarantine (DLQ) instead of main pipeline
            producer.send(DLQ_TOPIC, value=order)
            print(f"❌ BAD  | order_id={order['order_id'][:8]}... -> sent to DLQ | error_rate={error_rate:.1%}")

        # Check if we should trip (or reset) the breaker
        if error_rate > ERROR_THRESHOLD and not breaker_tripped:
            breaker_tripped = True
            print("\n🚨 CIRCUIT BREAKER TRIPPED! Error rate exceeded 2%.")
            print("   Pipeline paused conceptually — all bad data now quarantined in DLQ.\n")
            log_incident("TRIPPED", error_rate)
        elif error_rate <= ERROR_THRESHOLD and breaker_tripped:
            breaker_tripped = False
            print("\n✅ Circuit breaker RESET. Error rate back to normal.\n")
            log_incident("RESET", error_rate)