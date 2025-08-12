import os
import base64
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, expr,
    row_number, sha2, concat_ws, udf
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BinaryType
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# --- Spark session ---
spark = SparkSession.builder \
    .appName("OrdersLatestHistoryReprocessMask") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Schema ---
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# --- AES encryption helper ---
AES_KEY = get_random_bytes(16)  # store securely!
def encrypt_str(value: str) -> bytes:
    if value is None:
        return None
    cipher = AES.new(AES_KEY, AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(value.encode('utf-8'))
    return cipher.nonce + tag + ciphertext

encrypt_udf = udf(lambda v: encrypt_str(v), BinaryType())

# --- Kafka source ---
raw_orders = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_orders = raw_orders.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select(
    encrypt_udf(col("data.order_id")).alias("order_id_enc"),  # encrypted order_id
    col("data.status"),
    to_timestamp(col("data.event_time")).alias("event_time"),
    col("data.amount")
).withWatermark("event_time", "10 minutes")

# --- Dirty events quarantine ---
dirty_events = parsed_orders.filter(
    col("status").isNull() | col("event_time").isNull()
)
dirty_query = dirty_events.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_dirty") \
    .outputMode("append") \
    .start("/tmp/delta/orders_dirty")

# --- History table ---
history_query = parsed_orders \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_history") \
    .outputMode("append") \
    .start("/tmp/delta/orders_history")

# --- Latest table ---
window_spec = Window.partitionBy("order_id_enc").orderBy(col("event_time").desc())
latest_orders = parsed_orders.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

latest_query = latest_orders.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_latest") \
    .outputMode("complete") \
    .start("/tmp/delta/orders_latest")

# --- Late events capture ---
late_events = parsed_orders.filter(expr("event_time < current_timestamp() - interval 10 minutes"))
late_query = late_events.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_late") \
    .outputMode("append") \
    .start("/tmp/delta/orders_late")

# --- Dynamic reprocessing logic ---
def reprocess_late_events():
    """
    Reads late events Delta table and re-merges them into history & latest.
    This can be scheduled externally or triggered manually.
    """
    late_df = spark.read.format("delta").load("/tmp/delta/orders_late")
    if late_df.count() == 0:
        print("No late events to reprocess.")
        return

    # Append to history
    late_df.write.format("delta").mode("append").save("/tmp/delta/orders_history")

    # Merge into latest (overwrite matching order_id_enc if newer)
    from delta.tables import DeltaTable
    latest_tbl = DeltaTable.forPath(spark, "/tmp/delta/orders_latest")
    latest_tbl.alias("tgt").merge(
        late_df.alias("src"),
        "tgt.order_id_enc = src.order_id_enc"
    ).whenMatchedUpdate(
        condition="src.event_time > tgt.event_time",
        set={
            "status": "src.status",
            "event_time": "src.event_time",
            "amount": "src.amount"
        }
    ).whenNotMatchedInsertAll().execute()

    print(f"Reprocessed {late_df.count()} late events into latest/history.")

# Example: manual trigger for dynamic reprocessing
# reprocess_late_events()

spark.streams.awaitAnyTermination()
