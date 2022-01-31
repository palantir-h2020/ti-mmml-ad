from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from json import loads, dumps
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta
from collections import namedtuple
import os
import sys
import time
try:
    sys.path.index('%s/MIDAS.Python/src' % os.getcwd())
except ValueError:
    sys.path.append('%s/MIDAS.Python/src' % os.getcwd())
from MIDAS import FilteringCore, NormalCore, RelationalCore

if 'KAFKA_BROKER_IP' in os.environ:
    KAFKA_BROKER_IP = os.environ['KAFKA_BROKER_IP']
else:
    print('KAFKA_BROKER_IP env var required')
    exit()

if 'KAFKA_BROKER_PORT' in os.environ:
    KAFKA_BROKER_PORT = int(os.environ['KAFKA_BROKER_PORT'])
else:
    KAFKA_BROKER_PORT = 9092

if 'KAFKA_TOPIC_IN' in os.environ:
    KAFKA_TOPIC_IN = os.environ['KAFKA_TOPIC_IN']
else:
    print('KAFKA_TOPIC_IN env var required')
    exit()

if 'KAFKA_TOPIC_OUT' in os.environ:
    KAFKA_TOPIC_OUT = os.environ['KAFKA_TOPIC_OUT']
else:
    #KAFKA_TOPIC_OUT = 'netflow-demo-nec-ad-results'
    KAFKA_TOPIC_OUT = None

KAFKA_CONNECT_MAX_ATTEMPTS = 5
KAFKA_CONNECT_TIMEOUT = 60

def parse_cic_ids_ts(s):
    try:
        return datetime.strptime(s, '%d/%m/%Y %H:%M')
    except:
        pass

    try:
        return datetime.strptime(s, '%d/%m/%Y %H:%M:%S')
    except:
        pass

    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except:
        pass

    print('Cannot parse timestamp')
    raise ValueError

def prettyprint_result(is_anomalous, is_anomalous_GT):
    if is_anomalous:
        if is_anomalous_GT:
            print('\x1b[1;31;40m' + 'Anomaly (TP)!' + '\x1b[0m')
        else:
            print('\x1b[1;33;40m' + 'Anomaly (FP)!' + '\x1b[0m')
    else:
        if is_anomalous_GT:
            print('\x1b[1;33;40m' + 'Anomaly (FN)!' + '\x1b[0m')
        else:
            pass

def print_red(s):
    print('\x1b[1;31;40m' + s + '\x1b[0m')

def update_performance(is_anomalous, is_anomalous_GT, midas):
    if is_anomalous:
        if is_anomalous_GT:
            midas.tp += 1
        else:
            midas.fp += 1
    else:
        if is_anomalous_GT:
            midas.fn += 1
        else:
            midas.tn += 1

def send_alert(data, score, midas, producer):
    data_out = {'in_data': data.copy(),
                'method': midas.name,
                'score': score}
    producer.send(KAFKA_TOPIC_OUT, value=data_out)

def ip2int(ip):
    if '.' in ip:
        h = list(map(int, ip.split('.')))
        return (h[0]<<24) + (h[1]<<16) + (h[2]<<8) + (h[3]<<0)
    elif ':' in ip:
        # dummy conversion into 0,2*32 range
        return sum(list(map(lambda x: eval('0x%s' % x), filter(lambda x: x != '', ip.split(':'))))) % 2**32

def proc_msg(data, midas, producer):
    # Check if data has been correctly decoded
    if data is None:
        return

    # CSV-to-dict
    if True:
        data_split = data.split(',')
        data = {'timestamp': data_split[0],
                'source': data_split[3],
                'destination': data_split[4]}

    # Check if all required keys are found in data
    if len( set(['timestamp' ,'source' ,'destination']) - set(data.keys()) ) > 1:
        return

    tick = time.time()
    if midas.t0 is None:
        # The timestamp of the very first message is our time horizon
        try:
            midas.t0 = parse_cic_ids_ts(data['timestamp'])
        except ValueError:
            return
    src = ip2int(data['source'])
    dst = ip2int(data['destination'])
    # slots start from 1 to avoid ZeroDivisionError
    try:
        slot = int( (parse_cic_ids_ts(data['timestamp']) - midas.t0).total_seconds() // midas.slot_len.total_seconds() ) + 1
    except ValueError:
        return
    score = midas.core.Call(src, dst, slot)
    print('Processed in', time.time()-tick, 's')
    print('score =', score)

    is_anomalous = score > midas.score_thr
    if is_anomalous:
        print_red('Anomaly detected')
    if producer and is_anomalous:
        # NB we only report detections
        send_alert(data, score, midas, producer)

    print()

def init_midas():
    midas = namedtuple('MIDAS', [])
    midas.core = FilteringCore(2, 1024, 1e3)
    if os.environ.get('MIDAS_SLOT'):
        slot = os.environ['MIDAS_SLOT']
        if slot[-1] == 's':
            midas.slot_len = timedelta(seconds=int(slot[:-1]))
            midas.slot_len_str = '%ssec' % (slot[:-1])
        elif slot[-1] == 'm':
            midas.slot_len = timedelta(minutes=int(slot[:-1]))
            midas.slot_len_str = '%smin' % (slot[:-1])
        else:
            print('Invalid MIDAS_SLOT value:', MIDAS_SLOT)
            exit()
    else:
        midas.slot_len = timedelta(seconds=1)
        midas.slot_len_str = '1sec'
    if os.environ.get('MIDAS_THR'):
        midas.score_thr = int(os.environ['MIDAS_THR'])
    else:
        if midas.slot_len_str == '1min':
            midas.score_thr = 7062069 # 99.999p of Monday data score
        elif midas.slot_len_str == '2min':
            midas.score_thr = 1387058 # 99.999p of Monday data score
        elif midas.slot_len_str == '5min':
            midas.score_thr = 9114476 # 99.999p of Monday data score
        else:
            midas.score_thr = 8759679 # 99.999-th percentile of Scores from MIDAS-F(1 sec) on Monday-WorkingHours.pcap_ISCX.csv (i.e. train data)
    midas.t0 = None # Time horizon
    midas.name = 'MIDAS-F(slot: %s, score thr: %d)' % (midas.slot_len_str, midas.score_thr)

    return midas

def safe_deserializer_json(x):
    try:
        return loads(x.decode('utf-8'))
    except JSONDecodeError:
        print('Cannot parse JSON message')
        print(x)
        return None

def safe_deserializer_csv(x):
    # If for example no key is passed
    if x is None:
        return None

    try:
        return x.decode('utf-8')
    except UnicodeDecodeError:
        print('Cannot decode UTF-8 message')
        print(x)
        return None

if __name__ == "__main__":
    midas = init_midas()
    print('\x1b[1;32;40m' + midas.name + '\x1b[0m')

    attempt_num = 0
    is_connected = False
    while not is_connected:
        try:
            print('Connecting to Kafka broker %s:%d'% (KAFKA_BROKER_IP, KAFKA_BROKER_PORT))
            consumer = KafkaConsumer(
                 KAFKA_TOPIC_IN,
                 bootstrap_servers=['%s:%d' % (KAFKA_BROKER_IP, KAFKA_BROKER_PORT)],
                 group_id='group_%s' % midas.name, # set  non-None group_id to avoid consuming same events across re-runs of MIDAS. NB we include midas.name to make all instances consume all events.
                 auto_offset_reset='latest', # set to latest (instead of default earliest)
                 enable_auto_commit=True,
                 key_deserializer=safe_deserializer_csv,
                 value_deserializer=safe_deserializer_csv)
            is_connected = True
        except NoBrokersAvailable:
            attempt_num += 1
            if attempt_num < KAFKA_CONNECT_MAX_ATTEMPTS:
                print('Failed attempt %d/%d. Waiting %s sec...' % (attempt_num, KAFKA_CONNECT_MAX_ATTEMPTS, KAFKA_CONNECT_TIMEOUT))
                time.sleep(KAFKA_CONNECT_TIMEOUT)
            else:
                print('Failed attempt %d/%d. Max number of attempts exceeded' % (attempt_num, KAFKA_CONNECT_MAX_ATTEMPTS))
                exit()

    if KAFKA_TOPIC_OUT:
        # No need to catch NoBrokersAvailable if KafkaConsumer successfully connected
        # NB up to now KAFKA_TOPIC_OUT is based on JSON
        producer = KafkaProducer(
            bootstrap_servers=['%s:%d' % (KAFKA_BROKER_IP, KAFKA_BROKER_PORT)],
            value_serializer=lambda x: dumps(x).encode('utf-8'))
    else:
        producer = None

    print('Waiting for new messages from topic \'%s\'...' % KAFKA_TOPIC_IN)
    try:
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
            proc_msg(message.value, midas, producer)
    except KeyboardInterrupt:
        print('\nDone')

