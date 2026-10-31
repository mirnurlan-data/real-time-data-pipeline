# Kafka + Spark Structured Streaming — Reproducible Sales Event Pipeline

[![Kafka Spark Integration CI](https://github.com/mirnurlan-data/real-time-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/mirnurlan-data/real-time-data-pipeline/actions/workflows/ci.yml)

**Scope:** An educational, single-machine event pipeline with synthetic data. Kafka → PySpark Structured Streaming → Parquet sink + checkpoint → strict data quality gate.

See the [architecture and production trade-offs](docs/architecture.md) and the [known limitations](docs/limitations.md). **Do not interpret this as a production deployment or throughput benchmark.**

## Quick start

You need Docker Compose, Java 17, Python 3.11+, and network access to Maven packages.

```bash
python -m pip install -r requirements.txt
docker compose up -d --wait
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sales-events --partitions 3 --replication-factor 1
python producer/generate_events.py --count 60
SPARK_LOCAL_IP=127.0.0.1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 consumer/stream.py --once
SPARK_LOCAL_IP=127.0.0.1 python scripts/verify_output.py --expected 60
```

You should see `PASS` with 60 received and distinct records, and zero invalid rows. For continuously arriving events, omit `--once` and produce new messages while the consumer is running.

This lab uses `pyspark==3.5.3`, and the Maven Kafka connector must match that Spark/Scala combination.

**Repeat run:** Remove only the deliberately disposable `output/` directory (ignored by Git), reset the broker via `docker compose down -v` and repeat the commands. Existing checkpoints and prior events otherwise affect counts.

## Tests and evidence

- `python -m unittest discover -s tests -v` checks deterministic event generation
- GitHub Actions starts an isolated Kafka broker, publishes 60 synthetic events, runs Spark in `--once` mode and verifies Parquet records
- `output/validation.json` is uploaded as a CI artifact when generated
- No arbitrary speedups or latency claims are presented

The Kafka service exposes plaintext on loopback for local development only.

## Checkpoint recovery demonstration

The CI workflow starts the Spark consumer twice against the **same topic and existing checkpoint**, with no new messages between runs. The quality gate then requires exactly 60 unique events in the Parquet output, showing the demo does not append duplicates on this clean restart scenario. This is **not** a test of crashes during a micro-batch, broker failure, network partition, or exactly-once end-to-end guarantees.
