from kafka import KafkaProducer
from csv import reader
import os

# sudo apt install python3-pip
# pip3 install kafka-python

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

if 'CSV_FNAME' in os.environ:
    CSV_FNAME = os.environ['CSV_FNAME']
else:
    print('CSV_FNAME env var required')
    exit()

print('Connecting to Kafka broker %s:%d'% (KAFKA_BROKER_IP, KAFKA_BROKER_PORT))
producer = KafkaProducer(
    bootstrap_servers=['%s:%d' % (KAFKA_BROKER_IP, KAFKA_BROKER_PORT)],
    key_serializer=lambda x: x.encode('utf-8'),
    value_serializer=lambda x: x.encode('utf-8')
)

print('Reading %s...' % CSV_FNAME)
counter = 0
with open(CSV_FNAME, 'r') as file:
    for line in file:
        producer.send(topic=KAFKA_TOPIC, key='demo-message-%d' % counter, value=line.rstrip())
        producer.flush()
        counter = counter + 1
        #if counter == 1: break

print(str(counter) + ' netflows sent to Kafka!')
