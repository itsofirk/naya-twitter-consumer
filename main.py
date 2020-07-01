import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as dtypes

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
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    schema = dtypes.StructType()
    schema.add("id", dtypes.LongType())
    schema.add("text", dtypes.StringType())

    extended_tweet = dtypes.StructType()
    extended_tweet.add("full_text", dtypes.StringType())
    schema.add(dtypes.StructField("extended_tweet", extended_tweet))

    df = df.select(F.from_json(df.value.cast('string'), schema).alias('json')) \
        .select('json.*')

    df = df.select(df.id, F.when(df.extended_tweet.isNull(), df.text)
                   .otherwise(df.extended_tweet.full_text).alias('text'))

    # ds = df.writeStream \
    #     .option("checkpointLocation", 'D:\\tmp') \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "after") \
    #     .start()

    ds = df.writeStream \
        .option("truncate", "true") \
        .start(format='console')

    ds.awaitTermination()
