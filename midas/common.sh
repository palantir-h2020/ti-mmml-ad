#!/bin/bash

export KAFKA_BROKER_IP=NONE
export KAFKA_BROKER_PORT=9092
export KAFKA_TOPIC_NETFLOW_ANONYM_PREPROC=netflow-anonymized-preprocessed
export KAFKA_TOPIC_NETFLOW_RAW=netflow-raw
export KAFKA_TOPIC_NETFLOW_CSV=netflow-demo-nec # NOT pre-processed and NOT anonymized
export KAFKA_TOPIC_AD_RESULTS=netflow-demo-nec-ad-results
export KAFKA_TOOLS_PATH=NONE

export MIDAS_SLOT1=1m
export MIDAS_THR1=7062069 # 99.999p of Monday data score
#export MIDAS_SLOT2=5m
#export MIDAS_THR2=9114476 # 99.999p of Monday data score
export MIDAS_SLOT2=2m
export MIDAS_THR2=1387058 # 99.999p of Monday data score
