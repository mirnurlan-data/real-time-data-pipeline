"""Local Spark Structured Streaming consumer of Kafka sales events."""
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
    spark = (SparkSession.builder.master("local[2]")
             .appName("synthetic-sales-stream")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    try:
        # The Spark Kafka connector must match the installed Spark version.
        # Run with spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<matching-version>
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", "localhost:9092")
               .option("subscribe", "sales-events")
               .option("startingOffsets", "earliest")
               .load())
        parsed = (raw.select(from_json(col("value").cast("string"), SCHEMA).alias("event"))
                  .select("event.*")
                  .filter("event_id IS NOT NULL AND event_time IS NOT NULL AND quantity > 0"))
        clean = (parsed.withColumn("event_ts", to_timestamp("event_time"))
                 .withWatermark("event_ts", "10 minutes")
                 .dropDuplicates(["event_id", "event_ts"]))
        query = (clean.writeStream.format("parquet")
                 .option("path", str(BASE / "output" / "parquet"))
                 .option("checkpointLocation", str(BASE / "output" / "checkpoint"))
                 .outputMode("append").start())
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
