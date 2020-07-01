# Cons
import os

DELIMITERS = "[ \n\t]"
HASHTAGS = r'#.+'
MENTIONS = r'@.+'
DIGITS = r'\d+'
URLS = r'.+\:\/\/.+'
NON_ALPHABETICAL_WORDS = r'[^a-zA-Z]+'

KAFKA_JAR_LOCATION = os.path.join(os.environ.get('SPARK_HOME'), 'jars', 'spark-sql-kafka-0-10_2.11-2.4.5.jar')
