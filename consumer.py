from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime


spark = (SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
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
    .withColumnRenamed("s", "symbol") \
    .withColumnRenamed("p", "price") \
    .withColumnRenamed("q", "quantity") \
    .withColumnRenamed("f", "first_trade_id") \
    .withColumnRenamed("l", "last_trade_id") \
    .withColumnRenamed("T", "trade_time") \
    .withColumnRenamed("m", "is_market_maker")

df_table = df_table \
    .withColumn("trade_value", (col("price") * col("quantity")).cast(DecimalType(10, 4))) \
    .withColumn("delay_ms", col("event_time") - col("trade_time")) \
    .withColumn("event_time", from_unixtime(col("event_time") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")) \
    .withColumn("trade_time", from_unixtime(col("trade_time") / 1000, "yyyy-MM-dd HH:mm:ss.SSS")) \


windowedSum = df_table.groupBy(
    window(col("trade_time"), "10 minutes", "5 minutes"),
    col("symbol")
).agg(
    avg("trade_value").alias("avg_trade_value"),
    sum("trade_value").alias("total_trade_value"),
    count("agg_trade_id").alias("total_count_trade")
).orderBy(col("window").desc())


query = df_table \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start() \
    .awaitTermination()

""" query = windowedSum.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination() """
