# Real-Time Data Pipeline — Kafka + Spark Structured Streaming

A reproducible **educational streaming lab**: synthetic JSON sales events → Kafka topic → Spark Structured Streaming → console and Parquet sink. This example does **not** claim to be production-deployed or exactly-once end-to-end.

## Architecture
```text
Python event producer → Kafka (sales-events) → Spark Structured Streaming
                                             ↳ console output
                                             ↳ Parquet files + checkpoint
```

## Quick start
Requires Docker Compose, Python 3.10+, and sufficient memory for Kafka and Spark.

```bash
docker compose up -d --wait
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sales-events --partitions 3 --replication-factor 1
python -m pip install -r requirements.txt
python producer/generate_events.py --count 100
```

In a separate terminal run Spark using a local PySpark installation:

```bash
python consumer/stream.py
```

Start the consumer before the producer to observe the events live. The streaming consumer uses a local checkpoint directory (`output/checkpoint`) to recover state. The `output` directory is ignored by Git.

## Data contract
Each event includes `event_id`, `event_time`, `store_id`, `product_id`, `quantity`, and `unit_price`. All generated events are synthetic; there are no client data, cloud credentials, or secrets.

## Engineering trade-offs
- Kafka provides durable, partitioned ingress, but delivery guarantees depend on producer, broker, and sink settings.
- The demo writes to Parquet rather than PostgreSQL to avoid misrepresenting a guaranteed idempotent JDBC sink.
- The consumer adds a watermark and drops duplicate event IDs within watermark state; duplicates beyond the watermark can be processed again.
- Local Docker networking and `localhost` depend on where the consumer runs.
- Review [docs/limitations.md](docs/limitations.md) before extending this to production.

The included lightweight unit tests check event generation. A full Kafka/Spark integration test is **not** claimed.
