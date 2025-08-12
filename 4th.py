from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder \
    .appName("OrdersStreaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---- Schema ----
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

# ---- Kafka source ----
raw_orders = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# ---- Parse JSON ----
parsed_orders = raw_orders.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*") \
  .withWatermark("event_time", "10 minutes")  # late event handling

# ---- History table (append every event) ----
history_query = parsed_orders.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_history") \
    .outputMode("append") \
    .start("/tmp/delta/orders_history")

# ---- Latest table (1 row per order_id) ----
window_spec = Window.partitionBy("order_id").orderBy(col("event_time").desc())

latest_orders = parsed_orders \
    .withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

latest_query = latest_orders.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_latest") \
    .outputMode("complete") \
    .start("/tmp/delta/orders_latest")

spark.streams.awaitAnyTermination()


late_events = parsed_orders.filter(expr("event_time < current_timestamp() - interval 10 minutes"))

late_query = late_events.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/delta/checkpoints/orders_late") \
    .outputMode("append") \
    .start("/tmp/delta/orders_late")
