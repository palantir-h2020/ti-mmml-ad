from kafka_utils import *
from datetime import datetime, timedelta
from collections import namedtuple
import ipaddress
import os
import sys
import time
try:
    sys.path.index('%s/MIDAS.Python/src' % os.getcwd())
except ValueError:
    sys.path.append('%s/MIDAS.Python/src' % os.getcwd())
from MIDAS import FilteringCore, NormalCore, RelationalCore

if 'KAFKA_BROKERS_CSV' in os.environ:
    KAFKA_BROKERS_CSV = os.environ['KAFKA_BROKERS_CSV']
else:
    print('KAFKA_BROKERS_CSV env var required')
    exit()

if 'KAFKA_TOPIC_IN' in os.environ:
    KAFKA_TOPIC_IN = os.environ['KAFKA_TOPIC_IN']
else:
    print('KAFKA_TOPIC_IN env var required')
    exit()

if 'KAFKA_TOPIC_OUT' in os.environ:
    KAFKA_TOPIC_OUT = os.environ['KAFKA_TOPIC_OUT']
else:
    KAFKA_TOPIC_OUT = None

if 'KAFKA_TOPIC_OUT_FORMAT' in os.environ:
    KAFKA_TOPIC_OUT_FORMAT = os.environ['KAFKA_TOPIC_OUT_FORMAT']
else:
    KAFKA_TOPIC_OUT_FORMAT = 'csv'

if 'MIDAS_SLOT' in os.environ:
    MIDAS_SLOT = os.environ['MIDAS_SLOT']
else:
    MIDAS_SLOT = None

if 'MIDAS_THR' in os.environ:
    MIDAS_THR = os.environ['MIDAS_THR']
else:
    MIDAS_THR = None

if 'PROPAGATE_ANOMALIES_ONLY' in os.environ:
    PROPAGATE_ANOMALIES_ONLY = os.environ['PROPAGATE_ANOMALIES_ONLY']
    if PROPAGATE_ANOMALIES_ONLY == 'True':
        PROPAGATE_ANOMALIES_ONLY = True
    elif PROPAGATE_ANOMALIES_ONLY == 'False':
        PROPAGATE_ANOMALIES_ONLY = False
    else:
        print('PROPAGATE_ANOMALIES_ONLY env var has invalid value (True or False are allowed)')
        exit()
else:
    PROPAGATE_ANOMALIES_ONLY = False

if 'TRAINING' in os.environ:
    TRAINING = True
    if KAFKA_TOPIC_OUT is not None:
        print('[!] Ignoring KAFKA_TOPIC_OUT env var during training')
        KAFKA_TOPIC_OUT = None
    # TODO do we need other env vars (e.g. ELK endpoints)?
else:
    TRAINING = False

if 'VERBOSITY' in os.environ:
    if os.environ['VERBOSITY'] == 'DEBUG':
        VERBOSITY = logging.DEBUG
    elif os.environ['VERBOSITY'] == 'INFO':
        VERBOSITY = logging.INFO
    elif os.environ['VERBOSITY'] == 'WARNING':
        VERBOSITY = logging.WARNING
    elif os.environ['VERBOSITY'] == 'ERROR':
        VERBOSITY = logging.ERROR
    elif os.environ['VERBOSITY'] == 'CRITICAL':
        VERBOSITY = logging.CRITICAL
    else:
        print('VERBOSITY env var has invalid value')
        exit()
else:
    VERBOSITY = logging.INFO

logger = logging.getLogger('midas')
logger.setLevel(VERBOSITY)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(VERBOSITY)
logger.addHandler(consoleHandler)
formatter = logging.Formatter('%(asctime)s [%(module)s] %(levelname)s %(message)s')
consoleHandler.setFormatter(formatter)

################################################################################

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

    raise ValueError

def send_alert(data_dict, score, midas, producer, is_anomalous, propagate_to_ADaggr_only_anomalies=PROPAGATE_ANOMALIES_ONLY):
    global ad_msg_cnt
    if propagate_to_ADaggr_only_anomalies and not is_anomalous:
        return

    if KAFKA_TOPIC_OUT_FORMAT == 'json':
        data_out = {'in_data': data_dict.copy(),
                    'method': midas.name,
                    'score': score}
    else:
        data_out = data_dict['raw_netflow_data']
        data_out += ',MIDAS,"[]",%f,%d' % (score, is_anomalous)
        #data_out += ',MIDAS,%f,%d' % (score, is_anomalous)
    producer.send(KAFKA_TOPIC_OUT, key='MIDAS-AD-msg-%d' % ad_msg_cnt, value=data_out)
    ad_msg_cnt += 1

netflow_ftr_cnt_raw = 48 # netflow-raw
netflow_ftr_cnt_anonym_preproc = 62 # netflow-anonymized-preprocessed
valid_netflow_cnt_values = [netflow_ftr_cnt_raw, netflow_ftr_cnt_anonym_preproc]
def proc_msg(data, midas, producer):
    # Check if data has been correctly decoded (error already logged by kafka_utils)
    if data is None:
        return

    # CSV-to-dict
    data_split = data.split(',')
    data_split_len = len(data_split)
    if data_split_len not in valid_netflow_cnt_values:
        logger.error('Unexpected NetFlow format: got %d features, expected %s' % (data_split_len, valid_netflow_cnt_values))
        return
    # timestamp, source and destination have same position in both netflow-raw and netflow-anonymized-preprocessed
    data_dict = {'timestamp': data_split[0],
                 'source': data_split[3],
                 'destination': data_split[4],
                 'raw_netflow_data': data}

    tick = time.time()
    try:
        ts = parse_cic_ids_ts(data_dict['timestamp'])
        if midas.t0 is None:
            # The timestamp of the very first message determines the start of our time horizon
            midas.t0 = ts
            logger.info('Setting MIDAS time horizon start: %s' % ts)
    except ValueError:
        logger.error('Cannot parse timestamp (%s) from NetFlow event' % data_dict['timestamp'])
        return

    try:
        src = int(ipaddress.ip_address(data_dict['source'])) % 2**32
    except ValueError:
        logger.error('Cannot parse source IP (%s) from NetFlow event' % data_dict['source'])
        return

    try:
        dst = int(ipaddress.ip_address(data_dict['destination'])) % 2**32
    except ValueError:
        logger.error('Cannot parse destination IP (%s) from NetFlow event' % data_dict['destination'])
        return

    # slots start from 1 to avoid ZeroDivisionError
    try:
        slot = int( (ts - midas.t0).total_seconds() // midas.slot_len.total_seconds() ) + 1
    except Exception:
        logger.error('Cannot compute MIDAS slot')
        return

    score = midas.core.Call(src, dst, slot)
    logger.debug('Processed in %f s' % (time.time()-tick))
    logger.debug('score = %f' % score)

    is_anomalous = score > midas.score_thr
    if is_anomalous:
        logger.info('\x1b[1;31;40m Anomaly detected \x1b[0m')
    if producer:
        send_alert(data_dict, score, midas, producer, is_anomalous)

def init_midas(slot, thr):
    midas = namedtuple('MIDAS', [])
    midas.core = FilteringCore(2, 1024, 1e3)

    if slot is not None:
        if slot[-1] not in ['s', 'm']:
            logging.error('Invalid MIDAS_SLOT value (%s): slot should end with \'s\' or \'m\'' % slot)
            exit()
        try:
            if slot[-1] == 's':
                midas.slot_len = timedelta(seconds=int(slot[:-1]))
                midas.slot_len_str = '%ssec' % (slot[:-1])
            elif slot[-1] == 'm':
                midas.slot_len = timedelta(minutes=int(slot[:-1]))
                midas.slot_len_str = '%smin' % (slot[:-1])
        except ValueError:
            logging.error('Invalid MIDAS_SLOT value (%s): slot should start with an integer' % slot)
            exit()
    else:
        midas.slot_len = timedelta(seconds=1)
        midas.slot_len_str = '1sec'

    if thr is not None:
        try:
            midas.score_thr = int(thr)
        except ValueError:
            logging.error('Invalid MIDAS_THR value:', thr)
            exit()
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

if __name__ == "__main__":
    logger.info('KAFKA_BROKERS_CSV = %s' % KAFKA_BROKERS_CSV)
    logger.info('KAFKA_TOPIC_IN = %s' % KAFKA_TOPIC_IN)
    logger.info('KAFKA_TOPIC_OUT = %s' % KAFKA_TOPIC_OUT)
    logger.info('KAFKA_TOPIC_OUT_FORMAT = %s' % KAFKA_TOPIC_OUT_FORMAT)
    logger.info('MIDAS_SLOT = %s' % MIDAS_SLOT)
    logger.info('MIDAS_THR = %s' % MIDAS_THR)
    logger.info('PROPAGATE_ANOMALIES_ONLY = %s' % PROPAGATE_ANOMALIES_ONLY)
    logger.info('TRAINING = %s' % TRAINING)
    if VERBOSITY == logging.DEBUG:
        logger.info('VERBOSITY = DEBUG')
    elif VERBOSITY == logging.INFO:
        logger.info('VERBOSITY = INFO')
    elif VERBOSITY == logging.WARNING:
        logger.info('VERBOSITY = WARNING')
    elif VERBOSITY == logging.ERROR:
        logger.info('VERBOSITY = ERROR')
    elif VERBOSITY == logging.CRITICAL:
        logger.info('VERBOSITY = CRITICAL')

    midas = init_midas(MIDAS_SLOT, MIDAS_THR)
    logger.info('\x1b[1;32;40m' + midas.name + '\x1b[0m')

    ad_msg_cnt = 0
    # group_id='group_%s' % midas.name, # set  non-None group_id to avoid consuming same events across re-runs of MIDAS. NB we include midas.name to make all instances consume all events.
    consumer = build_kafka_consumer(KAFKA_BROKERS_CSV, KAFKA_TOPIC_IN, 'group_%s' % midas.name, 'csv', 'csv')
    if KAFKA_TOPIC_OUT:
        producer = build_kafka_producer(KAFKA_BROKERS_CSV, KAFKA_TOPIC_OUT_FORMAT, KAFKA_TOPIC_OUT_FORMAT)
    else:
        producer = None

    if not TRAINING:
        logger.info('Waiting for new messages from topic \'%s\'...' % KAFKA_TOPIC_IN)
        logger.info('{topic}:{partition}:{offset} key={key} value={value}')
        msg_cnt = 0
        try:
            for message in consumer:
                logger.info("%s:%d:%d key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
                proc_msg(message.value, midas, producer)
                msg_cnt += 1
        except KeyboardInterrupt:
            logger.info('Done')
        logger.info('Processed %d messages' % msg_cnt)
    else:
        # TODO
        # 1) understand how to get data from ELK: through Kafka? how to get the total number of msg to read?
        # 2) can we re-use proc_msg() ?
        # 3) as a quick test we would need to store ALL the scores to compute the percentile
        # 3.1) we might have a look at an approximate computation of percentiles as a steaming algorithm to avoid storing ALL of them
        pass
