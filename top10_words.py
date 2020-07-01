from os import path
import pyspark.sql.functions as F

import configs
import constants as C
import utils

APPNAME = 'naya-twitter-aggregation'


def write_to_console():
    ds = df.writeStream \
        .option("checkpointLocation", path.join(C.CHECKPOINT_LOCATION, APPNAME)) \
        .format("console") \
        .outputMode("complete") \
        .start()
    ds.awaitTermination()


if __name__ == '__main__':
    spark = utils.get_spark_session(APPNAME)
    spark.builder.config("spark.sql.shuffle.partitions", "8")

    df = utils.read_from_kafka(spark, configs.get_kafka_host(), configs.get_words_topic())

    df = df.selectExpr("CAST(value AS STRING)") \
        .withColumnRenamed('value', 'word')

    # Word count
    df = df.groupBy('word') \
        .count() \
        .orderBy(F.desc('count')) \
        .limit(10)

    write_to_console()
