import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

kafka_jar_location = os.path.join(os.environ.get('SPARK_HOME'), 'jars', 'spark-sql-kafka-0-10_2.11-2.4.5.jar')

if __name__ == "__main__":
    spark = SparkSession.builder.appName('naya-twitter-cleanse') \
        .config("spark.jars", kafka_jar_location) \
        .master('local[*]') \
        .getOrCreate()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets_raw") \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df = df.select(F.explode(F.split(df.value, " ")).alias("word"))

    ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "after") \
        .start()

    ds.awaitTermination()

