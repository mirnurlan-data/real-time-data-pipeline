"""Local Spark consumer. Use spark-submit with the version-matched Kafka connector."""
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

BASE = Path(__file__).resolve().parents[1]
SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("store_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])


def main():
    parser = argparse.ArgumentParser(description="Consume sales-events to local Parquet files")
    parser.add_argument("--once", action="store_true",
                        help="Process available offsets and stop (for reproducible CI)")
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    spark = (SparkSession.builder.master("local[2]")
             .appName("synthetic-sales-stream")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", "sales-events")
               .option("startingOffsets", "earliest")
               .load())
        parsed = (raw.select(from_json(col("value").cast("string"), SCHEMA).alias("event"))
                  .select("event.*")
                  .filter(col("event_id").isNotNull() &
                          col("event_time").isNotNull() &
                          col("store_id").isNotNull() &
                          col("product_id").isNotNull() &
                          (col("quantity") > 0) &
                          (col("unit_price") > 0)))
        clean = (parsed.withColumn("event_ts", to_timestamp("event_time"))
                 .filter(col("event_ts").isNotNull())
                 .withWatermark("event_ts", "10 minutes")
                 .dropDuplicates(["event_id", "event_ts"]))
        writer = (clean.writeStream.format("parquet")
                  .option("path", str(BASE / "output" / "parquet"))
                  .option("checkpointLocation", str(BASE / "output" / "checkpoint"))
                  .outputMode("append"))
        if args.once:
            writer = writer.trigger(availableNow=True)
        query = writer.start()
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
