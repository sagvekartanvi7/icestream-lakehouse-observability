import random
import time
import json
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Connect to our Kafka conveyor belt (running in Docker on localhost:9092)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "checkout-orders"


def create_fake_order():
    """
    Creates one fake e-commerce order.
    Sometimes (on purpose) makes tax_amount missing —
    this is the 'bad data' our system will later learn to catch.
    """
    order = {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "item": fake.word(),
        "price": round(random.uniform(5, 500), 2),
        "tax_amount": round(random.uniform(0.5, 50), 2),
        "country": fake.country(),
    }

    if random.random() < 0.10:
        order["tax_amount"] = None

    return order


if __name__ == "__main__":
    print(f"Sending fake orders to Kafka topic '{TOPIC_NAME}'... (Press Ctrl+C to stop)")

    while True:
        order = create_fake_order()
        producer.send(TOPIC_NAME, value=order)
        print("Sent:", order)
        time.sleep(1)  # wait 1 second between orders, so we can watch it happen