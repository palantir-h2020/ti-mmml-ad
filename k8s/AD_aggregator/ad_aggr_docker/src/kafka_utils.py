import json
from json.decoder import JSONDecodeError
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging
import time

logger = logging.getLogger('kafka_utils')
logger.setLevel(logging.INFO)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
logger.addHandler(consoleHandler)
formatter = logging.Formatter('%(asctime)s [%(module)s] %(levelname)s %(message)s')
consoleHandler.setFormatter(formatter)

# TODO clarify group_id, auto_offset_reset, enable_auto_commit
# TODO clarify bootstrap_servers
# TODO valide inputs
def build_kafka_consumer(brokers_csv, topic, group_id,
                         key_deserializer, value_deserializer,
                         max_connect_attempts=5, connect_retry_interval=60):
    assert key_deserializer in ['csv', 'json']
    assert value_deserializer in ['csv', 'json']

    attempt_num = 0
    is_connected = False
    bootstrap_servers=brokers_csv.split(',')
    while not is_connected:
        try:
            logger.info('Connecting to Kafka brokers %s as consumer for topic \'%s\''% (bootstrap_servers, topic))
            if topic is not None:
                consumer = KafkaConsumer(
                     topic,
                     bootstrap_servers=bootstrap_servers,
                     group_id=group_id,
                     auto_offset_reset='latest', # set to latest (instead of default earliest)
                     enable_auto_commit=True,
                     key_deserializer=safe_deserializer_csv if key_deserializer == 'csv' else safe_deserializer_json,
                     value_deserializer=safe_deserializer_csv if value_deserializer == 'csv' else safe_deserializer_json)
            else:
                consumer = KafkaConsumer(
                     bootstrap_servers=bootstrap_servers,
                     group_id=group_id,
                     auto_offset_reset='latest', # set to latest (instead of default earliest)
                     enable_auto_commit=True,
                     key_deserializer=safe_deserializer_csv if key_deserializer == 'csv' else safe_deserializer_json,
                     value_deserializer=safe_deserializer_csv if value_deserializer == 'csv' else safe_deserializer_json)
            is_connected = True
        except NoBrokersAvailable:
            attempt_num += 1
            if attempt_num < max_connect_attempts:
                logger.warning('Failed attempt %d/%d. Waiting %s sec...' % (attempt_num, max_connect_attempts, connect_retry_interval))
                time.sleep(connect_retry_interval)
            else:
                logger.error('Failed attempt %d/%d. Max number of attempts exceeded' % (attempt_num, max_connect_attempts))
                exit()

    return consumer

def build_kafka_producer(brokers_csv, key_serializer, value_serializer,
                         max_connect_attempts=5, connect_retry_interval=60):
    assert key_serializer in ['csv', 'json']
    assert value_serializer in ['csv', 'json']

    attempt_num = 0
    is_connected = False
    bootstrap_servers=brokers_csv.split(',')
    while not is_connected:
        try:
            logger.info('Connecting to Kafka brokers %s as producer'% (bootstrap_servers))
            producer = KafkaProducer(
                 bootstrap_servers=bootstrap_servers,
                 key_serializer=safe_serializer_csv if key_serializer == 'csv' else safe_serializer_json,
                 value_serializer=safe_serializer_csv if value_serializer == 'csv' else safe_serializer_json)
            is_connected = True
        except NoBrokersAvailable:
            attempt_num += 1
            if attempt_num < max_connect_attempts:
                logger.warning('Failed attempt %d/%d. Waiting %s sec...' % (attempt_num, max_connect_attempts, connect_retry_interval))
                time.sleep(connect_retry_interval)
            else:
                logger.error('Failed attempt %d/%d. Max number of attempts exceeded' % (attempt_num, max_connect_attempts))
                exit()

    return producer

def safe_deserializer_csv(x):
    if x is None:
        return None

    try:
        return x.decode('utf-8')
    except UnicodeDecodeError:
        logger.error('Cannot decode UTF-8 message')
        logger.error(x)
        return None

def safe_deserializer_json(x):
    if x is None:
        return None

    try:
        return json.loads(x.decode('utf-8'))
    except JSONDecodeError:
        logger.error('Cannot parse JSON message')
        logger.error(x)
        return None

def safe_serializer_csv(x):
    if x is None:
        return None

    try:
        return x.encode('utf-8')
    except UnicodeEncodeError:
        logger.error('Cannot encode UTF-8 message')
        logger.error(x)
        return None

def safe_serializer_json(x):
    if x is None:
        return None

    try:
        return json.dumps(x).encode('utf-8')
    except TypeError:
        logger.error('Cannot encode JSON message')
        logger.error(x)
        return None
