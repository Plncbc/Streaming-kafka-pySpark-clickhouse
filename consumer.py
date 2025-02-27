from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = (SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate())

schema = StructType([
    StructField("e", StringType(), True),  
    StructField("E", LongType(), True),    
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
    .selectExpr("CAST(value AS STRING) as json_string")

df_cleaned = df.withColumn("json_string", regexp_replace(col("json_string"), "\\\\\"", "\"")) \
    .withColumn("json_string", expr("SUBSTRING(json_string, 2, LENGTH(json_string) - 2)"))

df_parsed = df_cleaned.withColumn("data", from_json(col("json_string"), schema)) \
    .select("data.*")

query = df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='5 seconds') \
    .start() \
    .awaitTermination()

df_parsed.printSchema()