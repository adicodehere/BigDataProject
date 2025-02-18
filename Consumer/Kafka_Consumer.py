from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "manager"
MYSQL_DB = "stock_db"
MYSQL_TABLE = "stock_avg"


spark = SparkSession.builder \
    .appName("Kafka-Stock-Streaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars", "/path/to/mysql-connector-java.jar") \
    .getOrCreate()


kafka_topic = "stock"
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True),  
    StructField("volume", IntegerType(), True)
])


df = data.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))


result = df.withWatermark("timestamp", "10 seconds") \
    .groupBy("symbol", window("timestamp", "10 seconds")) \
    .agg(avg("price").alias("avg_price"), sum("volume").alias("volume")) \
    .select("symbol", "avg_price", "volume", col("window.start").alias("timestamp"))


def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .mode("append") \
        .save()


result.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()

spark.stop()
