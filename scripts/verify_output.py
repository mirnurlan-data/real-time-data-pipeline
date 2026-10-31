"""Assertions against materialized Parquet output of the local streaming demo."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]


def verify(expected: int) -> dict:
    if expected < 1:
        raise ValueError("expected must be positive")
    parquet = ROOT / "output" / "parquet"
    if not parquet.exists():
        raise FileNotFoundError("No Parquet sink output; did streaming finish?")
    spark = (SparkSession.builder.master("local[2]")
             .appName("streaming-data-quality")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        rows = spark.read.parquet(str(parquet))
        n = rows.count()
        unique = rows.select("event_id").distinct().count()
        invalid = rows.filter(
            F.col("event_id").isNull() |
            F.col("event_ts").isNull() |
            F.col("store_id").isNull() |
            F.col("product_id").isNull() |
            (F.col("quantity") <= 0) |
            (F.col("unit_price") <= 0)
        ).count()
        event_ids = set(x.event_id for x in rows.select("event_id").collect())
        expected_ids = {f"synthetic-{i:08d}" for i in range(1, expected + 1)}
        outcome = {"expected": expected, "received": n, "distinct_ids": unique,
                   "invalid_rows": invalid, "exact_id_match": event_ids == expected_ids}
        output = ROOT / "output" / "validation.json"
        output.write_text(json.dumps(outcome, indent=2) + "\n", encoding="utf-8")
        if n != expected or unique != expected or invalid != 0 or event_ids != expected_ids:
            raise AssertionError(f"Streaming data quality gate failed: {outcome}")
        print("PASS: " + json.dumps(outcome))
        return outcome
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", type=int, default=60)
    args = parser.parse_args()
    verify(args.expected)
