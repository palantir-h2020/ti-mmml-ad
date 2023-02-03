#!/usr/bin/env python3

import csv
import hashlib
from kafka_utils import *
import logging
import os
import sched
import threading
import time

'''
From https://confluence.i2cat.net/display/PAL/Data+and+Event+Streams+Definition

-Input topic: netflow-ad-tba.csv (i.e. to-be-aggregated)
CSV string: 62 NetFlow ftrs,MAD_module_name,MAD_module_input_ftrs,MAD_module_outlier_score,MAD_module_flag
MAD_module_input_ftrs is a CSV list enclosed in double quoted square brackets (e.g. "[]" or "[0.1,0.3,...]")
MAD_module_flag is 0 or 1

-Output topic: netflow-ad-out.csv (i.e. aggregated output)
CSV string: 12 NetFlow ftrs,MIDAS_OS,MIDAS_ADF,AE_OS,AE_ADF,GANomaly_OS,GANomaly_ADF,IFOREST_OS,IFOREST_ADF,33 AE/IFOREST-specific ftrs
The 12 NetFlow ftrs are ts,te,td,sa,da,sp,dp,pr,flg,stos,ipkt,ibyt
MIDAS_OS,MIDAS_ADF are the outlier score and the anomaly detection flag (None,None if no result has been provided)
'''

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

if 'AGGR_TIMEOUT' in os.environ:
    AGGR_TIMEOUT = int(os.environ['AGGR_TIMEOUT'])
else:
    AGGR_TIMEOUT = 30

if 'PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS' in os.environ:
    if os.environ['PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS'] == 'True':
        PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS = True
    elif os.environ['PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS'] == 'False':
        PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS = False
    else:
        print('PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS env var has invalid value')
        exit()
else:
    PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS = True

if 'EARLY_AGGREGATION' in os.environ:
    if os.environ['EARLY_AGGREGATION'] == 'ALL':
        EARLY_AGGREGATION = 'ALL'
    elif os.environ['EARLY_AGGREGATION'] == 'MIDAS_IFOREST':
        EARLY_AGGREGATION = 'MIDAS_IFOREST'
    elif os.environ['EARLY_AGGREGATION'] == 'IFOREST_GANomaly':
        EARLY_AGGREGATION = 'IFOREST_GANomaly'
    else:
        print('EARLY_AGGREGATION env var has invalid value')
        exit()
else:
    #EARLY_AGGREGATION = 'ALL'
    EARLY_AGGREGATION = 'MIDAS_IFOREST'
    #EARLY_AGGREGATION = 'IFOREST_GANomaly'

if 'DUMMY_CONSUMER' in os.environ:
    DUMMY_CONSUMER = True
else:
    DUMMY_CONSUMER = False

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

logger = logging.getLogger('ad_aggr')
logger.setLevel(VERBOSITY)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(VERBOSITY)
logger.addHandler(consoleHandler)
formatter = logging.Formatter('%(asctime)s [%(module)s] %(levelname)s %(message)s')
consoleHandler.setFormatter(formatter)

################################################################################

# Custom class required to add new events after sched.scheduler.run() is called
# [https://stackoverflow.com/a/64250589]
class scheduler_with_polling(sched.scheduler):
    def __init__(self, timefn, waitfn, **kwargs):
        super().__init__(timefn, waitfn)
        self.polling_interval = kwargs.get('polling_interval', 0.25)

    def run(self):
        self.enter(self.polling_interval, 1, self.__poll)
        super().run()

    def __poll(self):
        # logger.debug('%d scheduled events' % (len(self.queue)))
        self.enter(self.polling_interval, 1, self.__poll)

# Result of a single AD module
class ADresult():
    # TODO GANomaly-specific features count
    ad_module_specific_ftr_cnt = {'AE': 33, 'GANomaly': 0, 'IFOREST': 33, 'MIDAS': 0}

    def __init__(self, name, AD_data):
        self.name = name
        logger.debug('AD_data: %s' % AD_data)
        # AD_data.split(',') does not properly split '1,2,"[3,4]",5' into ['1','2','[3,4]','5'] as we need, csv.reader() does it.
        AD_data_split = next(csv.reader([AD_data], delimiter=',', quotechar='"'))
        logger.debug('AD_data_split: %s' % AD_data_split)
        if len(AD_data_split) == 3:
            ftrs, score, is_anomalous = AD_data_split
            if ftrs == '[]':
                ftrs_cnt = 0
            else:
                ftrs_cnt = len(ftrs.split(','))

            # We check the number of ad_module_specific_ftr even if we simply copy them w/o further processing
            if ftrs_cnt != ADresult.ad_module_specific_ftr_cnt[name]:
                logger.error('Unexpected number of ad_module_specific_ftr_cnt: got %d instead of %d' % (ftrs_cnt, ADresult.ad_module_specific_ftr_cnt[name]))
                raise Exception

            if ftrs_cnt == 0:
                self.ftrs = ''
            else:
                # We keep ftrs as CSV string w/o parsing them as float to avoid any rounding. [1:-1] slicing removes the square brackets
                self.ftrs = ftrs[1:-1]

            self.score = score
            self.is_anomalous = is_anomalous

            logger.debug(f'{self.name}_ftrs: {self.ftrs}')
            logger.debug(f'{self.name}_score: {self.score}')
            logger.debug(f'{self.name}_is_anomalous: {self.is_anomalous}')
        else:
            logger.error('Cannot parse AD_data')
            raise Exception

    def __repr__(self):
        pass

# Aggregated results from multiple AD modules
class AggrADresult():
    mad_modules_names = ['MIDAS', 'AE', 'GANomaly', 'IFOREST'] # Fixed serialization order expected by TCAM, do not change!
    netflow_ftr_cnt_raw = 48 # netflow-raw
    netflow_ftr_cnt_anonym_preproc_wo_zeek_and_SDA = 62 # netflow-anonymized-preprocessed
    netflow_ftr_cnt_anonym_preproc = 62+1+8 # netflow-anonymized-preprocessed (62 NetFlow ftrs + 1 Zeek ftr + 8 SDA ftrs)

    def __init__(self, key):
        self.key = key
        self.netflow_ftrs = None

        for ad_module_name in self.mad_modules_names:
            # self.[ad_module_name]_result = None
            setattr(self, '%s_result' % ad_module_name, None)

    @staticmethod
    def build_key(ad_event):
        ad_event_split = ad_event.split(',')
        if len(ad_event_split) < min(AggrADresult.netflow_ftr_cnt_raw, AggrADresult.netflow_ftr_cnt_anonym_preproc) + 1:
            # Either there are just the NetFlow features w/o AD result data or there are not even all the NetFlow features
            logger.error('Unexpected NetFlow format: cannot extract flow key (msg is too short)')
            return None

        # The name of the AD module is just after the NetFlow features
        if ad_event_split[AggrADresult.netflow_ftr_cnt_raw] in AggrADresult.mad_modules_names:
            netflow_ftr_cnt = AggrADresult.netflow_ftr_cnt_raw
        elif ad_event_split[AggrADresult.netflow_ftr_cnt_anonym_preproc] in AggrADresult.mad_modules_names:
            netflow_ftr_cnt = AggrADresult.netflow_ftr_cnt_anonym_preproc
        else:
            logger.error('Unexpected NetFlow format: cannot extract flow key (unrecognised NetFlow topic from the number of NetFlow features)')
            return None

        # AD_module_independent_ftrs = ad_event_split[:netflow_ftr_cnt]
        # Since for each flow we get duplicated messages (both with 71 ftrs but one with SDA ftrs set and the other with Zeek)
        # we use as aggregation key the "old" 62 ftrs
        AD_module_independent_ftrs = ad_event_split[:AggrADresult.netflow_ftr_cnt_anonym_preproc_wo_zeek_and_SDA]
        logger.debug("AD_module_independent_ftrs: %s" % AD_module_independent_ftrs)
        AD_module_independent_ftrs = ''.join(AD_module_independent_ftrs)
        # TODO we could simply use return AD_module_independent_ftrs as key or '|'.join() to make them human readable
        # key = AD_module_independent_ftrs
        key = hashlib.sha1(AD_module_independent_ftrs.encode('utf-8')).hexdigest()[:8]

        return key

    def __create_ADresult(self, name, AD_data):
        try:
            return ADresult(name, AD_data)
        except Exception:
            logger.error('%s AD result not stored' % name)
            return None

    def add_single_AD_result(self, ad_event):
        ad_event_split = ad_event.split(',')
        if len(ad_event_split) < min(AggrADresult.netflow_ftr_cnt_raw, AggrADresult.netflow_ftr_cnt_anonym_preproc) + 1:
            logger.error('Unexpected NetFlow format: cannot process single AD result (msg is too short)')
            return False

        if ad_event_split[self.netflow_ftr_cnt_raw] in self.mad_modules_names:
            netflow_ftr_cnt = self.netflow_ftr_cnt_raw
        elif ad_event_split[self.netflow_ftr_cnt_anonym_preproc] in self.mad_modules_names:
            netflow_ftr_cnt = self.netflow_ftr_cnt_anonym_preproc
        else:
            logger.error('Unexpected NetFlow format: cannot process single AD result (unrecognised NetFlow topic from the number of NetFlow features)')
            return False

        # If an AD_result has been already previously added, self.netflow_ftrs would be already set
        if self.netflow_ftrs is None:
            self.netflow_ftrs = ad_event_split[:netflow_ftr_cnt]

        ad_module_name = ad_event_split[netflow_ftr_cnt]
        ad_module_specific_data = ','.join(ad_event_split[netflow_ftr_cnt + 1:])
        if ad_module_name not in self.mad_modules_names:
            logger.error('Unknown AD module name \'%s\'' % ad_module_name)
            return False

        # ad_module_result = self.[ad_module_name]_result
        ad_module_result = getattr(self, '%s_result' % ad_module_name)
        if ad_module_result is None:
            # self.[ad_module_name]_result = self.__create_ADresult(ad_module_name, ad_module_specific_data)
            setattr(self, '%s_result' % ad_module_name, self.__create_ADresult(ad_module_name, ad_module_specific_data))
        else:
            logger.warning('%s AD score already received for this key' % ad_module_name)

        return True

    # THESE TWO LISTS ARE CONSISTENTLY SORTED!
    def __all_AD_name(self):
        return self.mad_modules_names
    def __all_AD_results(self):
        # return [ self.[X]_result for X in self.__all_AD_name() ]
        return [ getattr(self, '%s_result' % ad_module_name) for ad_module_name in self.__all_AD_name() ]

    def __all_AD_results__is_anomalous(self):
        d = []
        for AD_result in self.__all_AD_results():
            if AD_result is not None:
                d.append(AD_result.is_anomalous)

        return d

    def early_aggregation_possible(self):
        if EARLY_AGGREGATION == 'ALL':
            if any(result is None for result in self.__all_AD_results()):
                logger.debug('Early aggregation for key %s NOT possible: still waiting to get a response from at least 1 MAD module' % (self.key))
                return False
        elif EARLY_AGGREGATION == 'MIDAS_IFOREST':
            if any(result is None for result in [self.MIDAS_result, self.IFOREST_result]):
                if self.MIDAS_result is None and self.IFOREST_result is None:
                    logger.debug('MIDAS_IFOREST early aggregation for key %s NOT possible: still waiting to get a response from MIDAS & IFOREST' % (self.key))
                elif self.MIDAS_result is None and self.IFOREST_result is not None:
                    logger.debug('MIDAS_IFOREST early aggregation for key %s NOT possible: still waiting to get a response from MIDAS' % (self.key))
                else:
                    logger.debug('MIDAS_IFOREST early aggregation for key %s NOT possible: still waiting to get a response from IFOREST' % (self.key))
                return False
        elif EARLY_AGGREGATION == 'IFOREST_GANomaly':
            if any(result is None for result in [self.IFOREST_result, self.GANomaly_result]):
                if self.IFOREST_result is None and self.GANomaly_result is None:
                    logger.debug('IFOREST_GANomaly early aggregation for key %s NOT possible: still waiting to get a response from IFOREST & GANomaly' % (self.key))
                elif self.IFOREST_result is None and self.GANomaly_result is not None:
                    logger.debug('IFOREST_GANomaly early aggregation for key %s NOT possible: still waiting to get a response from IFOREST' % (self.key))
                else:
                    logger.debug('IFOREST_GANomaly early aggregation for key %s NOT possible: still waiting to get a response from GANomaly' % (self.key))
                return False

        return True

    def shall_propagate_to_TCAM(self):
        # If at least one method detected an anomaly...
        if any(is_anomalous == '1' for is_anomalous in self.__all_AD_results__is_anomalous()):
            if not PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS:
                return True
            else:
                if self.AE_result is not None or self.IFOREST_result is not None:
                    # ... AND we have AE/IF features, we propagate
                    return True
                else:
                    logger.debug('At least one MAD module detected an anomaly but no AE/IF features are available...')
                    logger.debug('...and PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS=True.')
                    return False

        return False

    def __repr__(self):
        r = f'AggrADresult(key={self.key}'

        for ad_module_name in self.__all_AD_name():
            # result = self.[ad_module_name]_result
            result = getattr(self, '%s_result' % ad_module_name)
            if result:
                r += f',{ad_module_name}_score={result.score}({result.is_anomalous})'
            else:
                r += f',{ad_module_name}_score=None'

        r += ')'

        return r

    def serialize(self):
        csv_str_listed = []
        # append ts,te,td,sa,da,sp,dp,pr,flg,stos,ipkt,ibyt NetFlow features
        csv_str_listed += self.netflow_ftrs[:9]
        csv_str_listed += self.netflow_ftrs[10:13]

        # for each MIDAS/AE/GANomaly/IFOREST method append {score},{is_anomalous}
        for name,r in zip(self.__all_AD_name(),
                          self.__all_AD_results()):
            if r is not None:
                csv_str_listed += [r.score, r.is_anomalous]
            else:
                csv_str_listed += [None, None]

        csv_str = ','.join(map(str, csv_str_listed))

        # append AE or IF features
        if self.AE_result is not None and self.IFOREST_result is not None:
            csv_str += ',' + self.AE_result.ftrs
        elif self.AE_result is not None and self.IFOREST_result is None:
            csv_str += ',' + self.AE_result.ftrs
        elif self.AE_result is None and self.IFOREST_result is not None:
            csv_str += ',' + self.IFOREST_result.ftrs
        else:
            logger.warning('Cannot append AE/IFOREST features (not found)...')
            pass

        logger.debug(csv_str)

        return csv_str

def process_ad_event(ad_event, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer, lock):
    key = AggrADresult.build_key(ad_event)
    if key is None:
        return

    logger.debug('process_ad_event(%s)' % (key))

    with lock:
        # Create or retrieve aggregated AD results
        if key not in aggr_ad_results_by_key:
            aggr_ad_result = AggrADresult(key)
            aggr_ad_results_by_key[key] = aggr_ad_result
        else:
            aggr_ad_result = aggr_ad_results_by_key[key]

        added = aggr_ad_result.add_single_AD_result(ad_event)
        if not added:
            return

        # We check whether we can do early aggregation BEFORE scheduling a future aggregation with scheduler.enter().
        # In the extreme case, an early aggregation might be performed as soon a single, specific, MAD module detects an anomaly.
        # We want thus to avoid calling scheduler.cancel() immediatly after scheduler.enter()...
        # It should be noted that an early aggregation does not imply a propagation to TCAM (this is evaluated within aggregate_ad_events()).
        if aggr_ad_result.early_aggregation_possible():
            logger.debug('%s early aggregation for key %s' % (EARLY_AGGREGATION, key))
            if key in sch_events_by_key:
                scheduler.cancel( sch_events_by_key[key] )
            # We do NOT call locked_aggregate_ad_events() because we are already inside a 'with lock' block
            aggregate_ad_events(key, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer)
        else:
            # Schedule aggregation (only if key is new)
            if key not in sch_events_by_key:
                logger.debug('Scheduling event for key %s' % (key))
                sch_ev = scheduler.enter(AGGR_TIMEOUT, 1,
                                         locked_aggregate_ad_events,
                                         (key, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer, lock))
                sch_events_by_key[key] = sch_ev

def aggregate_ad_events(key, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer):
    global aggr_msg_cnt

    logger.debug('aggregate_ad_events(%s)' % (key))
    logger.info('Processing %s' % aggr_ad_results_by_key[key])

    # Retrieve AD results and remove it from helper dicts
    results = aggr_ad_results_by_key.pop(key)
    if key in sch_events_by_key:
        _ = sch_events_by_key.pop(key)

    if results.shall_propagate_to_TCAM():
        aggr_ad_results_csv = results.serialize()
        if producer is not None:
            logger.info('\x1b[1;31;40mSending msg #%d on topic %s\x1b[0m' % (aggr_msg_cnt, KAFKA_TOPIC_OUT))
            producer.send(topic=KAFKA_TOPIC_OUT, key='AD-aggr-message-%d' % aggr_msg_cnt, value=aggr_ad_results_csv)
            aggr_msg_cnt += 1
        else:
            logger.debug('Kafka Producer not configured (missing KAFKA_TOPIC_OUT)')
    else:
        logger.debug('AD results NOT propagated')

def locked_aggregate_ad_events(key, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer, lock):
    with lock:
        aggregate_ad_events(key, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer)

if __name__ == "__main__":
    logger.info('KAFKA_BROKERS_CSV = %s' % KAFKA_BROKERS_CSV)
    logger.info('KAFKA_TOPIC_IN = %s' % KAFKA_TOPIC_IN)
    logger.info('KAFKA_TOPIC_OUT = %s' % KAFKA_TOPIC_OUT)
    logger.info('AGGR_TIMEOUT = %d' % int(AGGR_TIMEOUT))
    logger.info('PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS = %s' % PROPAGATE_ONLY_WITH_AE_OR_IFOREST_FTRS)
    logger.info('EARLY_AGGREGATION = %s' % EARLY_AGGREGATION)
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
    logger.info('\x1b[1;32;40mAD aggregator module started (AGGR_TIMEOUT: %d)\x1b[0m' % (AGGR_TIMEOUT))

    scheduler = scheduler_with_polling(time.time, time.sleep)
    sch_events_by_key = {}
    aggr_ad_results_by_key = {}
    lock = threading.Lock()
    aggr_msg_cnt = 0

    consumer = build_kafka_consumer(KAFKA_BROKERS_CSV, KAFKA_TOPIC_IN, 'group_ADaggr', 'csv', 'csv')
    if KAFKA_TOPIC_OUT:
        producer = build_kafka_producer(KAFKA_BROKERS_CSV, 'csv', 'csv')
    else:
        producer = None

    logger.info('Starting scheduling thread')
    scheduler_t = threading.Thread(target=scheduler.run)
    scheduler_t.start()

    logger.info('Waiting for new messages from topic \'%s\'...' % KAFKA_TOPIC_IN)
    msg_cnt = 0
    try:
        for message in consumer:
            if DUMMY_CONSUMER:
                logger.info("%s:%d:%d: key=%s [consumed]" % (message.topic, message.partition,
                                              message.offset, message.key))
            else:
                logger.debug("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
                process_ad_event(message.value, scheduler, sch_events_by_key, aggr_ad_results_by_key, producer, lock)
            msg_cnt += 1
    except KeyboardInterrupt:
        logger.info('Done')
    logger.info('Processed %d messages' % msg_cnt)

    # Wait for scheduler thread
    scheduler_t.join()

    logger.info('AD aggregator module stopped')
