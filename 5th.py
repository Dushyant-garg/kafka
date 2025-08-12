# orders_metrics_stream.py
# Spark Structured Streaming job with live metrics to console and HTML.
#
# Requires: Delta, PySpark, Kafka
# Usage:
#   spark-submit --packages io.delta:delta-core_2.12:2.3.0 orders_metrics_stream.py

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, count, avg, max as spark_max, sum as spark_sum,
    window, lit
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# ==== Config ====
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "order_status"
OUTPUT_METRICS_HTML = "/tmp/delta/metrics_report.html"

spark = SparkSession.builder \
    .appName("OrdersMetricsStream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema (extend with price/amount if needed)
schema = StructType([
    StructField("order_id", StringType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),   # parse to timestamp
    StructField("amount", DoubleType())        # for sales metric
])

# Read from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.order_id"),
        col("data.status"),
        to_timestamp(col("data.event_time")).alias("event_time"),
        col("data.amount")
    )

# Separate dirty rows (invalid/missing)
dirty_df = parsed_df.filter(col("order_id").isNull() | col("status").isNull() | col("event_time").isNull())
clean_df = parsed_df.exceptAll(dirty_df)

# Add processing time
clean_df = clean_df.withColumn("proc_time", lit(datetime.utcnow()))

# Compute metrics per microbatch
def compute_metrics(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    total_records = batch_df.count()
    throughput = total_records / 5.0  # if trigger interval is 5s
    max_event_time = batch_df.agg(spark_max("event_time")).collect()[0][0]
    lag_sec = (datetime.utcnow() - max_event_time.replace(tzinfo=None)).total_seconds() if max_event_time else None
    state_counts = batch_df.groupBy("status").count().collect()
    daily_sales = batch_df.groupBy(window(col("event_time"), "1 day")).agg(spark_sum("amount").alias("total_sales")).collect()

    # Error rate
    dirty_count = dirty_df.count()
    error_rate = dirty_count / (total_records + dirty_count)

    # Console output
    print(f"\n=== Batch {batch_id} Metrics ===")
    print(f"Throughput: {throughput:.2f} recs/sec")
    print(f"Lag: {lag_sec:.2f} sec" if lag_sec is not None else "Lag: N/A")
    print("State counts:", state_counts)
    print("Daily sales:", daily_sales)
    print(f"Error rate: {error_rate*100:.2f}%")

    # HTML output
    html = f"""
    <html><head><title>Streaming Metrics</title></head><body>
    <h1>Streaming Metrics (Batch {batch_id})</h1>
    <p><b>Throughput:</b> {throughput:.2f} recs/sec</p>
    <p><b>Lag:</b> {lag_sec:.2f} sec</p>
    <p><b>Error rate:</b> {error_rate*100:.2f}%</p>
    <h2>State counts</h2>
    <ul>
    {''.join(f'<li>{row["status"]}: {row["count"]}</li>' for row in state_counts)}
    </ul>
    <h2>Daily Sales</h2>
    <ul>
    {''.join(f'<li>{row["window"]["start"]} - {row["total_sales"]}</li>' for row in daily_sales)}
    </ul>
    <p>Last updated: {datetime.utcnow()}</p>
    </body></html>
    """
    os.makedirs(os.path.dirname(OUTPUT_METRICS_HTML), exist_ok=True)
    with open(OUTPUT_METRICS_HTML, "w") as f:
        f.write(html)

metrics_query = clean_df.writeStream \
    .foreachBatch(compute_metrics) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

metrics_query.awaitTermination()
