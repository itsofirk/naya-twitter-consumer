from os import path
import pyspark.sql.functions as F
import pyspark.sql.types as dtypes
import utils
import constants as C
import configs

APPNAME = 'naya-twitter-cleanse'


def write_to_kafka(host, topic, key_value_df):
    ds = key_value_df.writeStream \
        .option("checkpointLocation", path.join(C.CHECKPOINT_LOCATION, APPNAME)) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", host) \
        .option("topic", topic) \
        .start()

    ds.awaitTermination()


def parse_tweets_json(raw_df):
    raw_string_df = raw_df.selectExpr("CAST(value AS STRING)")
    schema = dtypes.StructType([
        dtypes.StructField("id", dtypes.LongType()),
        dtypes.StructField("text", dtypes.StringType()),
        dtypes.StructField("extended_tweet", dtypes.StructType([
            dtypes.StructField("full_text", dtypes.StringType())
        ]))
    ])
    with_nulls_df = raw_string_df.select(F.from_json(raw_string_df.value, schema).alias('json')) \
        .select('json.*')
    return with_nulls_df.select(with_nulls_df.id,
                                F.when(with_nulls_df.extended_tweet.isNull(), with_nulls_df.text)
                                .otherwise(with_nulls_df.extended_tweet.full_text).alias('tweet'))


def _get_complete_pattern(*sub_patterns):
    s = ''
    for ptn in sub_patterns:
        s += f'^({ptn})$|'
    return s[:-1]


def data_cleanse(tweets_df):
    # Split content to words
    words_df = tweets_df.select(tweets_df.id, F.explode(F.split(tweets_df.tweet, C.DELIMITERS)).alias('word'))
    # Remove tags - moved into the regex string
    # Remove non-relevant “words” like web addresses, numbers only, words without alphabetical characters
    _pattern = _get_complete_pattern(C.HASHTAGS, C.DIGITS, C.URLS, C.NON_ALPHABETICAL_WORDS, C.MENTIONS)
    words_df = words_df.filter(~words_df.word.rlike(_pattern))
    # Remove non alphabetical characters from all words, for example: the word “Hi,” will be changed to “Hi”
    return words_df.withColumn('word', F.regexp_replace(words_df.word, r'\W', '')) \
        .filter(words_df.word != '')


if __name__ == "__main__":
    spark = utils.get_spark_session(APPNAME)

    df = utils.read_from_kafka(spark, configs.get_kafka_host(), configs.get_raw_topic())
    parsed_df = parse_tweets_json(df)
    only_words_df = data_cleanse(parsed_df)

    # Preparation for kafka
    df = only_words_df.withColumnRenamed('word', 'value') \
        .withColumn('key', F.lit(None).cast('string'))

    write_to_kafka(configs.get_kafka_host(), configs.get_words_topic(), df)
