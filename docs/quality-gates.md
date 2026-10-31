# Streaming adversarial data-quality tests

The GitHub Actions integration test sends **64 synthetic messages** into Kafka: 60 valid events, one exact duplicate, one negative-quantity event, one event with null ID, and one payload that is a JSON string rather than a JSON object.

Spark's JSON parser and row filters reject the invalid events. Watermark-based deduplication is expected to remove the duplicate in this *bounded event-time window*. The Parquet quality gate requires exactly the 60 expected IDs, each unique, with no invalid fields. The consumer is then restarted with the same checkpoint and the same 60-record assertion is repeated.

This demonstrates **bounded validation, deduplication and clean restart** for one synthetic run. It does **not** prove exactly-once end-to-end delivery, rejection of all schema errors, cross-watermark deduplication, or crash consistency. The test does not measure throughput, load capacity or error rates.
