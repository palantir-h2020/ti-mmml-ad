#!/bin/bash

set -o xtrace

source common.sh

DIR=$(pwd)
cd ~/kafka_2.13-3.0.0/bin

./kafka-console-producer.sh --bootstrap-server ${KAFKA_BROKER_IP}:${KAFKA_BROKER_PORT} --topic ${KAFKA_TOPIC_NETFLOW_CSV}

cd ${DIR}
