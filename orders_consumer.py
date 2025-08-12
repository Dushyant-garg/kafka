from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp, window, count

# --------------------------------
# 1. Spark Session
# --------------------------------
spark = SparkSession.builder \
    .appName("KafkaOrdersConsumer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------
# 2. Define Schema
# --------------------------------
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("timestamp", StringType(), False)  # incoming as string
])

# --------------------------------
# 3. Read from Kafka
# --------------------------------
orders_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# --------------------------------
# 4. Parse JSON and Extract Event Time
# --------------------------------
orders_parsed = orders_raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# --------------------------------
# 5. Quarantine Dirty Records
# --------------------------------
valid_orders = orders_parsed.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("event_time").isNotNull() &
    (col("quantity") > 0) &
    (col("price") > 0)
)

dirty_orders = orders_parsed.subtract(valid_orders)

dirty_query = dirty_orders.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/chk_dirty_orders") \
    .outputMode("append") \
    .start("/tmp/delta/dirty_orders")

# --------------------------------
# 6. Deduplicate with Watermark
# --------------------------------
deduped_orders = valid_orders \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["order_id", "event_time"])

# --------------------------------
# 7. Persist Clean Data
# --------------------------------
clean_query = deduped_orders.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/chk_clean_orders") \
    .outputMode("append") \
    .start("/tmp/delta/clean_orders")

# --------------------------------
# 8. Demonstrate Watermark Effect
# --------------------------------
windowed_counts = valid_orders \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes")) \
    .agg(count("*").alias("order_count"))

windowed_query = windowed_counts.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

# --------------------------------
# 9. Wait for Termination
# --------------------------------
spark.streams.awaitAnyTermination()
