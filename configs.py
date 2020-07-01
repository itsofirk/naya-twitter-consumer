import argparse
from configparser import ConfigParser

_parser = argparse.ArgumentParser()
_parser.add_argument('-c', '--config', dest='config', help='Path to the ini config file', required=True)
_args = _parser.parse_args()

c = ConfigParser()
c.read(_args.config)


def get_kafka_host():
    return c.get('kafka', 'kafka_host')


def get_raw_topic():
    return c.get('kafka', 'raw_topic')


def get_words_topic():
    return c.get('kafka', 'words_topic')
