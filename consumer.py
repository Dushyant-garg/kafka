from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("timestamp", StringType(), False)
])

status_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("timestamp", StringType(), False)
])

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaOrdersConsumer") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


orders_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

status_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order_status") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


from pyspark.sql.functions import col, from_json

orders_df = orders_raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*") \
.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())

status_df = status_raw.select(
    from_json(col("value").cast("string"), status_schema).alias("data")
).select("data.*") \
.filter(col("order_id").isNotNull() & col("status").isNotNull())


orders_query = orders_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/chk_orders") \
    .outputMode("append") \
    .start("/tmp/delta/orders")

status_query = status_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/chk_status") \
    .outputMode("append") \
    .start("/tmp/delta/status")


spark.streams.awaitAnyTermination()

| Parameter                      | Value                                                                                                 | Purpose                                  |
| ------------------------------ | ----------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `kafka.bootstrap.servers`      | `localhost:9092`                                                                                      | Kafka broker connection                  |
| `subscribe`                    | `orders` / `order_status`                                                                             | Kafka topics consumed                    |
| `startingOffsets`              | `earliest`                                                                                            | Ensures all historical data is processed |
| `failOnDataLoss`               | `false`                                                                                               | Avoids job failure if Kafka data is lost |
| `spark.sql.shuffle.partitions` | `4`                                                                                                   | Parallelism for stateful operations      |
| `checkpointLocation`           | `/tmp/chk_orders` / `/tmp/chk_status`                                                                 | Stores progress & recovery metadata      |
| `outputMode`                   | `append`                                                                                              | New rows only (no updates/deletes)       |
| Output Format                  | `delta` / `parquet`                                                                                   | Storage format for processed data        |
| Schema Enforcement             | JSON schema                                                                                           | Ensures only valid data is written       |
| Validation Rules               | `order_id` & `customer_id` must not be null (orders), `order_id` & `status` must not be null (status) | Basic data quality checks                |
