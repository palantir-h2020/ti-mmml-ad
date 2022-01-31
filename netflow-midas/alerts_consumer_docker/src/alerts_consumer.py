from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from json import loads
from json.decoder import JSONDecodeError
import os
import time

if 'KAFKA_BROKER_IP' in os.environ:
    KAFKA_BROKER_IP = os.environ['KAFKA_BROKER_IP']
else:
    print('KAFKA_BROKER_IP env var required')
    exit()

if 'KAFKA_BROKER_PORT' in os.environ:
    KAFKA_BROKER_PORT = int(os.environ['KAFKA_BROKER_PORT'])
else:
    KAFKA_BROKER_PORT = 9092

if 'KAFKA_TOPIC' in os.environ:
    KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
else:
    print('KAFKA_TOPIC env var required')
    exit()

KAFKA_CONNECT_MAX_ATTEMPTS = 5
KAFKA_CONNECT_TIMEOUT = 60

def safe_deserializer(x):
    try:
        return loads(x.decode('utf-8'))
    except JSONDecodeError:
        print('Cannot parse JSON message')
        print(x)
        return None

if __name__ == "__main__":
    print('\x1b[1;32;40m' + '[ALERTS CONSUMER]' + '\x1b[0m')

    attempt_num = 0
    is_connected = False
    while not is_connected:
        try:
            print('Connecting to Kafka broker %s:%d'% (KAFKA_BROKER_IP, KAFKA_BROKER_PORT))
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=['%s:%d' % (KAFKA_BROKER_IP, KAFKA_BROKER_PORT)],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=safe_deserializer)
            is_connected = True
        except NoBrokersAvailable:
            attempt_num += 1
            if attempt_num < KAFKA_CONNECT_MAX_ATTEMPTS:
                print('Failed attempt %d/%d. Waiting %s sec...' % (attempt_num, KAFKA_CONNECT_MAX_ATTEMPTS, KAFKA_CONNECT_TIMEOUT))
                time.sleep(KAFKA_CONNECT_TIMEOUT)
            else:
                print('Failed attempt %d/%d. Max number of attempts exceeded' % (attempt_num, KAFKA_CONNECT_MAX_ATTEMPTS))
                exit()

    print('Waiting for new messages from topic \'%s\'...' % KAFKA_TOPIC)
    try:
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
            #proc_msg(message.value, midas, producer)
            #print(message)
            print()
    except KeyboardInterrupt:
        print('\nDone')
