#!/bin/bash

set -o xtrace

source common.sh

if [ $# -eq 0 ]
  then
    echo "KAFKA_TOPIC arg required"
    exit
fi

DIR=$(pwd)
cd data_ingestion

if [[ "$2" != "LOCAL_KAFKA" ]]; then
   KAFKA_BROKER_IP=${KAFKA_BROKER_IP} KAFKA_BROKER_PORT=${KAFKA_BROKER_PORT} KAFKA_TOPIC=$1 CSV_FNAME="demo-v2.csv" python3 ingest_csv_data_as_csv_events.py
else
   KAFKA_BROKER_IP=localhost KAFKA_BROKER_PORT=9093 KAFKA_TOPIC=$1 CSV_FNAME="demo-v2.csv" python3 ingest_csv_data_as_csv_events.py
fi

cd ${DIR}
