# Kafka + Spark Structured Streaming — Reproducible Sales Event Pipeline

[![Kafka Spark Integration CI](https://github.com/mirnurlan-data/real-time-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mirnurlan-data/real-time-data-pipeline/actions/workflows/ci.yml)

A reproducible **synthetic** event streaming lab: Kafka → Spark Structured Streaming → Parquet and checkpoint → strict data quality checks.

[Architecture](docs/architecture.md) · [Adversarial test methodology](docs/quality-gates.md) · [Limitations](docs/limitations.md)

## Quick start

Requires Docker Compose, Java 17, Python 3.11+, and Maven package network access.

```bash
python -m pip install -r requirements.txt
docker compose up -d --wait
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sales-events --partitions 3 --replication-factor 1
python producer/generate_events.py --count 60 --adversarial
SPARK_LOCAL_IP=127.0.0.1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 consumer/stream.py --once
SPARK_LOCAL_IP=127.0.0.1 python scripts/verify_output.py --expected 60
```

In adversarial mode, the producer publishes **60 valid + 6 intentionally duplicate/malformed/invalid events**. The expected Parquet output remains 60 unique valid IDs. Start from a clean local `output/` directory and reset the disposable Kafka instance to reproduce the exact count; existing checkpoints or topic messages affect it.

## What CI actually verifies

- Deterministic producer contract unit tests
- A real single-broker Kafka deployment started in GitHub Actions
- Spark 3.5.3 parsing and processing the Kafka topic with matching connector artifact
- Data quality filtering, bounded deduplication, and exact expected event IDs
- Clean Spark restart with the existing checkpoint and no additional output records
- Machine-readable validation evidence uploaded as an artifact

This is an **educational local integration test**, not evidence of production high availability, security, end-to-end exactly-once guarantees or measured scale. For production guidance, see [architecture](docs/architecture.md) and [limitations](docs/limitations.md).
