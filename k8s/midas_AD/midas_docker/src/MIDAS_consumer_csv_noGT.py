from kafka import TopicPartition
from kafka_utils import *
from datetime import datetime, timedelta
from collections import namedtuple
import ipaddress
import numpy as np
from opensearchpy import OpenSearch
import os
import requests
import sys
import threading
import time
import traceback
try:
    sys.path.index('%s/MIDAS.Python/src' % os.getcwd())
except ValueError:
    sys.path.append('%s/MIDAS.Python/src' % os.getcwd())
from MIDAS import FilteringCore

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

if 'OPENSEARCH_API_URL' in os.environ:
    OPENSEARCH_API_URL = os.environ['OPENSEARCH_API_URL']
else:
    OPENSEARCH_API_URL = None

if 'OPENSEARCH_API_AUTH' in os.environ:
    OPENSEARCH_API_AUTH = os.environ['OPENSEARCH_API_AUTH']
else:
    OPENSEARCH_API_AUTH = None

if 'RETRAINING_PERIOD' in os.environ:
    RETRAINING_PERIOD = os.environ['RETRAINING_PERIOD']
    try:
        RETRAINING_PERIOD = int(RETRAINING_PERIOD)
    except ValueError:
        logging.error('Invalid RETRAINING_PERIOD value:', RETRAINING_PERIOD)
        exit()

    if OPENSEARCH_API_URL is None or OPENSEARCH_API_AUTH is None:
        logging.error('Both OPENSEARCH_API_URL and OPENSEARCH_API_AUTH are required for periodic re-training')
        exit()
else:
    RETRAINING_PERIOD = 0

# TODO handle RETRAINING_DATA_HORIZON

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

if 'TENANT_ID' in os.environ:
    TENANT_ID = os.environ['TENANT_ID']
    if TENANT_ID != 'ANY_TENANT' and not TENANT_ID.isnumeric():
        print('TENANT_ID env var must be either ANY_TENANT or a number')
        exit()

    if TENANT_ID == 'ANY_TENANT':
        # No need to check/use TENANT_SERVICE_API_URL
        TENANT_SERVICE_API_URL = None
    else:
        if 'TENANT_SERVICE_API_URL' in os.environ:
            TENANT_SERVICE_API_URL = os.environ['TENANT_SERVICE_API_URL']
        else:
            print('TENANT_SERVICE_API_URL env var required')
            exit()
else:
    TENANT_ID = 'ANY_TENANT'
    TENANT_SERVICE_API_URL = None

if TENANT_ID == 'ANY_TENANT':
    STORED_MIDAS_THR_FNAME = '/midas_volume/MIDAS_THR.MIDAS_SLOT_%s.ANY_TENANT.thr' % (MIDAS_SLOT)
    STORED_MIDAS_THR_TS_FNAME = '/midas_volume/MIDAS_THR.MIDAS_SLOT_%s.ANY_TENANT.ts' % (MIDAS_SLOT)
else:
    STORED_MIDAS_THR_FNAME = '/midas_volume/MIDAS_THR.MIDAS_SLOT_%s.TENANT_ID_%s.thr' % (MIDAS_SLOT, TENANT_ID)
    STORED_MIDAS_THR_TS_FNAME = '/midas_volume/MIDAS_THR.MIDAS_SLOT_%s.TENANT_ID_%s.ts' % (MIDAS_SLOT, TENANT_ID)
if os.path.isfile(STORED_MIDAS_THR_FNAME) and os.path.isfile(STORED_MIDAS_THR_TS_FNAME):
    with open(STORED_MIDAS_THR_FNAME, 'r') as f:
        STORED_MIDAS_THR = f.read().strip()
    with open(STORED_MIDAS_THR_TS_FNAME, 'r') as f:
        STORED_MIDAS_THR_TS = f.read().strip()
else:
    STORED_MIDAS_THR = None
    STORED_MIDAS_THR_TS = None

if TENANT_ID == 'ANY_TENANT':
    OPENSEARCH_INDEX = 'netflow-preprocessed-index-new'
else:
    OPENSEARCH_INDEX = 'netflow-preprocessed-index-new'
    # TODO once index name has been clarified!
    # OPENSEARCH_INDEX = 'netflow-preprocessed-index-new.%s' % (TENANT_ID)

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
logger.propagate = False # avoid propagating from child thread to parent
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(VERBOSITY)
logger.addHandler(consoleHandler)
formatter = logging.Formatter('%(asctime)s [%(module)s.%(threadName)s] %(levelname)s %(message)s')
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
    global PARTITION_ID
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
    if TENANT_ID == 'ANY_TENANT':
        producer.send(KAFKA_TOPIC_OUT, key='MIDAS-AD-msg-%d' % ad_msg_cnt, value=data_out)
    else:
        producer.send(KAFKA_TOPIC_OUT, key='MIDAS-AD-msg-%d' % ad_msg_cnt, value=data_out, partition=PARTITION_ID)
    ad_msg_cnt += 1

def run_data_into_midas(m, data_dict):
    try:
        ts = parse_cic_ids_ts(data_dict['timestamp'])
        if m.t0 is None:
            # The timestamp of the very first message determines the start of our time horizon
            m.t0 = ts
            logger.info('Setting MIDAS time horizon start: %s' % ts)
    except ValueError:
        logger.error('Cannot parse timestamp (%s) from NetFlow event' % data_dict['timestamp'])
        return None

    try:
        src = int(ipaddress.ip_address(data_dict['source'])) % 2**32
    except ValueError:
        logger.error('Cannot parse source IP (%s) from NetFlow event' % data_dict['source'])
        return None

    try:
        dst = int(ipaddress.ip_address(data_dict['destination'])) % 2**32
    except ValueError:
        logger.error('Cannot parse destination IP (%s) from NetFlow event' % data_dict['destination'])
        return None

    # slots start from 1 to avoid ZeroDivisionError
    try:
        slot = int( (ts - m.t0).total_seconds() // m.slot_len.total_seconds() ) + 1
    except Exception:
        logger.error('Cannot compute MIDAS slot')
        return None

    score = m.core.Call(src, dst, slot)

    return score

netflow_ftr_cnt_raw = 48 # netflow-raw
# netflow_ftr_cnt_anonym_preproc = 62 # netflow-anonymized-preprocessed
netflow_ftr_cnt_anonym_preproc = 62+1+8 # netflow-anonymized-preprocessed (62 NetFlow ftrs + 1 Zeek ftr + 8 SDA ftrs)
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
    score = run_data_into_midas(midas, data_dict)
    if score is None:
        return
    logger.debug('Processed in %f s' % (time.time()-tick))
    logger.debug('score = %f' % score)

    is_anomalous = score > midas.score_thr
    if is_anomalous:
        logger.info('\x1b[1;31;40m Anomaly detected \x1b[0m')
    if producer:
        send_alert(data_dict, score, midas, producer, is_anomalous)

def proc_opensearch_raw_data(data):
    # CSV-to-dict
    data_split = data.split(',')
    data_split_len = len(data_split)
    if data_split_len not in valid_netflow_cnt_values:
        return None

    # timestamp, source and destination have same position in both netflow-raw and netflow-anonymized-preprocessed
    data_dict = {'timestamp': data_split[0],
                 'source': data_split[3],
                 'destination': data_split[4],
                }

    # include parsed timestamp as datetime to enable sorting afterwards
    return [parse_cic_ids_ts(data_dict['timestamp']), data_dict]

def init_midas(slot, thr, stored_thr):
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

    if stored_thr is not None:
        try:
            stored_thr_ = int(stored_thr)
        except ValueError:
            logging.error('Invalid STORED_MIDAS_THR value:', stored_thr)
            stored_thr_ = None
    else:
        stored_thr_ = None

    if thr is not None:
        try:
            thr_ = int(thr)
        except ValueError:
            logging.error('Invalid MIDAS_THR value:', thr)
            thr_ = None
    else:
        thr_ = None

    if stored_thr_ is not None and thr_ is not None:
        # When available, we prefer to use a value from storage than the one provided by the user
        # because, in principle, it comes from a previous re-training round so it's more tailored to the current deployment
        logging.info('Setting threshold to STORED_MIDAS_THR')
        midas.score_thr = stored_thr_
    elif stored_thr_ is None and thr_ is not None:
        logging.info('Setting threshold to MIDAS_THR')
        midas.score_thr = thr_
    elif stored_thr_ is not None and thr_ is None:
        logging.info('Setting threshold to STORED_MIDAS_THR')
        midas.score_thr = stored_thr_
    else:
        # Both are invalid or not provided: fallback to default values
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

def get_historical_data_from_opensearch():
    OPENSEARCH_API_HOST = OPENSEARCH_API_URL.split(':')[0]
    OPENSEARCH_API_PORT = OPENSEARCH_API_URL.split(':')[1]
    client = OpenSearch(
        hosts = [{'host': OPENSEARCH_API_HOST, 'port': OPENSEARCH_API_PORT}],
        http_compress = True,
        http_auth = OPENSEARCH_API_AUTH.split(':'),
        use_ssl = True,
        verify_certs = False,
        ssl_show_warn = False
    )

    query = {}
    response = client.count(
        body = query,
        index = OPENSEARCH_INDEX
    )
    logger.info('%s documents found in index \'%s\'' % (response['count'], OPENSEARCH_INDEX))
    print()

    # TODO filter by date based on RETRAINING_DATA_HORIZON (no need to sort here, done afterwards in any case)
    query = {
        'size': 100
    }
    response = client.search(
        body = query,
        index = OPENSEARCH_INDEX
    )

    if 'hits' in response and 'hits' in response['hits']:
        results = response['hits']['hits']
    else:
        logger.error()
        return []

    parsed_results = []
    for r in results:
        if not ('_source' in r and 'network' in r['_source'] and 'raw' in r['_source']['network']):
            continue
        raw_data = r['_source']['network']['raw']
        parsed_result = proc_opensearch_raw_data(raw_data)
        if parsed_result is not None:
            parsed_results.append( parsed_result )

    return parsed_results

def periodic_retraining_thread_fx(curr_midas):
    logger.info('...done!')
    while True:
        logger.info('Waiting %d seconds...' % (RETRAINING_PERIOD))
        time.sleep(RETRAINING_PERIOD)
        logger.info('...starting periodic re-training.')

        # Create a new MIDAS instance for re-training, with the same params of the main MIDAS instance
        logger.info('Creating support MIDAS instance...')
        midas_r = init_midas(MIDAS_SLOT, MIDAS_THR, STORED_MIDAS_THR)

        # Read data from OpenSearch from the latest X days/months, pass it through MIDAS and store all the scores
        historical_data = get_historical_data_from_opensearch()
        logger.info('OpenSearch returned %s results' % len(historical_data))
        if len(historical_data) == 0:
            logger.error('No results found: skipping re-training round!')
            continue
        scores = []
        # sort historical_data by timestamp (very important for MIDAS, which implements a streaming algorithm!)
        for r in sorted(historical_data, key=lambda x: x[0]):
            data_dict = r[1]
            score = run_data_into_midas(midas_r, data_dict)
            if score is None:
                continue
            # TODO we could use approximate computation of percentiles as a steaming algorithm to avoid storing ALL of them
            scores.append(score)

        # Compute the 99.999-th percentile
        curr_threshold = curr_midas.score_thr
        new_threshold = int(np.percentile(scores, 99.999))
        # Update score_thr in the main MIDAS instance
        # (we do not need a Lock because the main thread created the instance and then only read score_thr, with no more updates)
        curr_midas.score_thr = new_threshold
        new_ts = '%s' % (datetime.now())
        logger.info('min/avg/max MIDAS scores: %.1f/%.1f/%.1f' % (min(scores),np.average(scores),np.max(scores)))
        logger.info('Updating MIDAS threshold %d -> %d (%s)%s' % (curr_threshold, new_threshold, new_ts, ' [NO CHANGES!]' if curr_threshold==new_threshold else ''))
        del scores
        # Update threshold in persistent storage
        with open(STORED_MIDAS_THR_FNAME, 'w') as f:
            f.write('%s' % (new_threshold))
        with open(STORED_MIDAS_THR_TS_FNAME, 'w') as f:
            f.write(new_ts)

if __name__ == "__main__":
    logger.info('# Kafka parameters')
    logger.info('KAFKA_BROKERS_CSV = %s' % KAFKA_BROKERS_CSV)
    logger.info('KAFKA_TOPIC_IN = %s' % KAFKA_TOPIC_IN)
    logger.info('KAFKA_TOPIC_OUT = %s' % KAFKA_TOPIC_OUT)
    logger.info('KAFKA_TOPIC_OUT_FORMAT = %s' % KAFKA_TOPIC_OUT_FORMAT)
    logger.info('# Kafka parameters for multi-tenancy')
    logger.info('TENANT_SERVICE_API_URL = %s' % TENANT_SERVICE_API_URL)
    logger.info('TENANT_ID = %s' % TENANT_ID)
    logger.info('# MIDAS parameters')
    logger.info('MIDAS_SLOT = %s' % MIDAS_SLOT)
    logger.info('MIDAS_THR = %s' % MIDAS_THR)
    logger.info('STORED_MIDAS_THR_FNAME = %s' % STORED_MIDAS_THR_FNAME)
    logger.info('STORED_MIDAS_THR = %s (%s)' % (STORED_MIDAS_THR, STORED_MIDAS_THR_TS))
    logger.info('# Periodic re-training parameters')
    logger.info('RETRAINING_PERIOD = %s' % RETRAINING_PERIOD)
    logger.info('OPENSEARCH_API_URL = %s' % OPENSEARCH_API_URL)
    logger.info('OPENSEARCH_API_AUTH = %s' % OPENSEARCH_API_AUTH)
    logger.info('# Other parameters')
    logger.info('PROPAGATE_ANOMALIES_ONLY = %s' % PROPAGATE_ANOMALIES_ONLY)
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

    midas = init_midas(MIDAS_SLOT, MIDAS_THR, STORED_MIDAS_THR)
    logger.info('\x1b[1;32;40m' + midas.name + '\x1b[0m')

    if RETRAINING_PERIOD > 0:
        logger.info('Starting periodic re-training thread...')
        t = threading.Thread(target=periodic_retraining_thread_fx, args=(midas,), name='RetrainingThread')
        t.start()
    else:
        logger.info('Periodic re-training not enabled (RETRAINING_PERIOD = 0)')

    ad_msg_cnt = 0
    PARTITION_ID = None

    # group_id='group_%s' % midas.name, # set  non-None group_id to avoid consuming same events across re-runs of MIDAS. NB we include midas.name to make all instances consume all events.
    if TENANT_ID == 'ANY_TENANT':
        consumer = build_kafka_consumer(KAFKA_BROKERS_CSV, KAFKA_TOPIC_IN, 'group_%s' % midas.name, 'csv', 'csv')
    else:
        try:
            logger.info('HTTP GET %s/%s' % (TENANT_SERVICE_API_URL, TENANT_ID))
            r = requests.get('%s/%s' % (TENANT_SERVICE_API_URL, TENANT_ID))
            PARTITION_ID = r.json()['partition'] # int
            logger.info('PARTITION_ID = %d' % PARTITION_ID)
        except:
            logger.error('Cannot retrieve PARTITION_ID for PARTITION_ID %s' % TENANT_ID)
            traceback.print_exc()
            exit()
        # topic and group_id set to None, not compatible with assign()
        consumer = build_kafka_consumer(KAFKA_BROKERS_CSV, None, None, 'csv', 'csv')
        consumer.assign([TopicPartition(KAFKA_TOPIC_IN, PARTITION_ID)])
    # Explicit partitions for KafkaProducer() are set in send() fx, no changes are needed here
    if KAFKA_TOPIC_OUT:
        producer = build_kafka_producer(KAFKA_BROKERS_CSV, KAFKA_TOPIC_OUT_FORMAT, KAFKA_TOPIC_OUT_FORMAT)
    else:
        producer = None

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
