from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime


spark = (SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,com.clickhouse:clickhouse-jdbc:0.8.4") \
    .config("spark.sql.streaming.checkpointLocation", "./SparkCheckpoint") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate())

schema = StructType([
    StructField("e", StringType(), True),
    StructField("event_time", LongType(), True),
    StructField("a", LongType(), True),
    StructField("s", StringType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("f", LongType(), True),
    StructField("l", LongType(), True),
    StructField("T", LongType(), True),
    StructField("m", BooleanType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:19092") \
    .option("subscribe", "binance_agg_trade") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(col("value").cast("string").alias("json_string"))\

df_string = df \
    .withColumn("json_string", expr("replace(json_string, '\\\\\"', '\"')")) \
    .withColumn("json_string", expr("replace(json_string, '\"E\":', '\"event_time\":')")) \
    .withColumn("json_string", expr("SUBSTRING(json_string, 2, LENGTH(json_string) - 2)")) \
    .withColumn("data", from_json(col("json_string"), schema)) \

df_table = df_string.select("data.*") \
    .withColumnRenamed("e", "event_type") \
    .withColumnRenamed("a", "agg_trade_id") \
    .withColumnRenamed("s", "traded_pair") \
    .withColumnRenamed("p", "price") \
    .withColumnRenamed("q", "quantity") \
    .withColumnRenamed("f", "first_trade_id") \
    .withColumnRenamed("l", "last_trade_id") \
    .withColumnRenamed("T", "trade_time") \
    .withColumnRenamed("m", "is_market_maker")

df_table = df_table \
    .where(df_table.agg_trade_id.isNotNull()) \
    .withColumn("price", col("price").cast(DecimalType(12, 3))) \
    .withColumn("quantity", col("quantity").cast(DecimalType(12, 4))) \
    .withColumn("trade_value", (col("price") * col("quantity")).cast(DecimalType(20, 6))) \
    .withColumn("delay_ms", col("event_time") - col("trade_time")) \
    .withColumn("event_time", from_unixtime(col("event_time") / 1000, "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("trade_time", from_unixtime(col("trade_time") / 1000, "yyyy-MM-dd HH:mm:ss")) \

url = "jdbc:ch://clickhouse-server:8123/default"
user = "user"
password = "123"  
driver = "com.clickhouse.jdbc.ClickHouseDriver"
table_name = "from_pySpark"

def write_to_clickhouse(batch_df, epoch_id):
    batch_df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable",table_name) \
        .option("isolationLevel", "NONE")\
        .mode("append") \
        .save()

query = df_table \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime='20 seconds') \
    .foreachBatch(write_to_clickhouse) \
    .start() \
    .awaitTermination()