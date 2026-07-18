# IceStream — Real-Time Lakehouse Observability

A self-healing streaming data pipeline that detects bad data in real time, automatically quarantines it, and keeps a permanent, time-travelable record of every change — without any human intervention.

Built as a hands-on implementation of a modern data lakehouse architecture, using Kafka for streaming, Apache Iceberg for storage, and a custom circuit breaker for automated data quality enforcement.

## The problem

Traditional batch ETL pipelines introduce hours of delay. When bad data (nulls, schema drift, malformed records) enters a pipeline, it's often not caught until a downstream dashboard breaks — sometimes during a critical business moment. IceStream solves this by monitoring data quality **as it streams**, not after the fact.

## How it works

```
generate_orders.py  →  Kafka (checkout-orders)  →  circuit_breaker.py  →  Iceberg lakehouse
     (producer)             (message broker)          (quality watchman)      (permanent storage)
                                                              ↓
                                                    checkout-orders-dlq
                                                    (quarantined bad records)
```

1. **Producer** (`generate_orders.py`) simulates a live e-commerce checkout stream, publishing orders to Kafka. ~10% of records are intentionally malformed (missing `tax_amount`) to simulate real-world data quality issues.
2. **Circuit breaker** (`circuit_breaker.py`) consumes the stream and evaluates each record against a data quality rule. It tracks the error rate over a rolling window of the last 20 records. If the error rate exceeds a 2% threshold, it automatically:
   - Routes bad records to a separate **dead letter queue** (`checkout-orders-dlq`) instead of the main pipeline
   - Logs a timestamped incident record (`incident_log.csv`) with the reason for the trip
   - Resets automatically once data quality recovers
3. **Lakehouse** (`write_to_lakehouse.py`) batches clean records and writes them permanently into an **Apache Iceberg** table, creating a new immutable snapshot on every write.
4. **Time travel** (`time_travel.py`) demonstrates Iceberg's snapshot isolation — querying the exact state of the data at any historical point, even after thousands of new records have been added since.

## Key concepts demonstrated

- **Streaming ingestion** with Apache Kafka (producer/consumer pattern)
- **Automated data quality enforcement** using a sliding-window error-rate circuit breaker
- **Dead letter queue (DLQ) pattern** for isolating bad data without stopping the pipeline
- **Open table format (Apache Iceberg)** for ACID-compliant writes and time-travel queries on top of local file storage
- **Observability** through persistent, auditable incident logging

## Tech stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka (via Docker) |
| Stream processing | Python (`kafka-python`) |
| Storage format | Apache Iceberg (`pyiceberg`) |
| Data quality checks | Custom Python rule engine (rolling error-rate circuit breaker) |
| Fake data generation | `Faker` |

## Project structure

```
icestream-lakehouse-observability/
├── generate_orders.py       # Kafka producer - simulates live checkout stream
├── circuit_breaker.py       # Data quality watchman with automated DLQ routing + logging
├── setup_lakehouse.py       # Initializes the Iceberg catalog and table schema
├── write_to_lakehouse.py    # Batches and writes clean data into Iceberg
├── time_travel.py           # Demonstrates Iceberg snapshot time-travel queries
├── check_data_quality.py    # Standalone data quality checker (early prototype)
├── docker-compose.yml       # Kafka + Zookeeper local cluster
├── incident_log.csv         # Generated at runtime - audit trail of circuit breaker events
└── requirements.txt
```

## Running it locally

**Prerequisites:** Docker Desktop, Python 3.11

```bash
# 1. Start Kafka
docker compose up -d

# 2. Set up Python environment
python -m venv venv
venv\Scripts\Activate.ps1        # Windows
pip install -r requirements.txt

# 3. Initialize the lakehouse
python setup_lakehouse.py

# 4. Run the pipeline (in separate terminals)
python generate_orders.py        # producer
python circuit_breaker.py        # quality watchman
python write_to_lakehouse.py     # lakehouse writer

# 5. Explore time travel
python time_travel.py
```

## What I learned building this

This project was built end-to-end, including working through real environment setup challenges: Python version compatibility issues with newer data libraries, Windows/WSL2 configuration for Docker, and adapting code to breaking API changes across `pyiceberg` versions. Debugging these issues firsthand gave practical experience with the kind of environment and dependency management that comes up constantly in real data engineering work.

## Possible next steps

- Replace the custom rule engine with **Great Expectations** for more comprehensive validation rules
- Replace the Python consumer loop with **Apache Flink** for true stream processing at scale
- Build a live **React Flow** dashboard connected via WebSockets for real-time pipeline visualization
- Add schema drift detection alongside null-value detection

---

*Inspired by the "IceStream" project brief from Axlero Solutions' Advanced Data Analytics curriculum.*