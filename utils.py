import constants as C

from pyspark.sql import SparkSession, DataFrame


def get_spark_session(app_name):
    return SparkSession.builder.appName(app_name) \
        .config("spark.jars", C.KAFKA_JAR_LOCATION) \
        .master('local[*]') \
        .getOrCreate()


def read_from_kafka(session: SparkSession, host, topic, starting_offset='earliest') -> DataFrame:
    return session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", host) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offset) \
        .load()
