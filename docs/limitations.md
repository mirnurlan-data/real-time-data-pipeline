# Architecture notes and limitations

This portfolio project is a local education example, not a production system.

- **Kafka:** Single broker, plaintext listener, single replica. Never deploy as-is to production.
- **Spark:** The Kafka package is not bundled automatically with the `pyspark` Python package. Run via `spark-submit` with an artifact matching your Spark and Scala versions.
- **Starting offsets:** `earliest` applies only when there is no compatible checkpoint.
- **Deduplication:** Uses event ID plus parsed event time within a 10-minute watermark, not global lifetime uniqueness.
- **Output:** Parquet files and checkpoint; the source diagram is not a JDBC/PostgreSQL sink.
- **Schema:** Basic null filtering is not production-grade validation, and `unit_price` uses floating point for illustration.
- **Capacity:** No measured throughput, latency SLO, or exactly-once end-to-end claim.
- **Deployment:** Test on a disposable local machine before using any cloud service.
