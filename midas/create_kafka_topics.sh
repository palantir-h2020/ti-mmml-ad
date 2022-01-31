#!/bin/bash

set -o xtrace

source common.sh

DIR=$(pwd)

cd ${KAFKA_TOOLS_PATH}/bin

# Topic deletion might not work when Kafka property delete.topic.enable is set to true.
# It seems --delete removes at least all the events from the topic.
if [[ "$1" != "AD_RESULTS_ONLY" ]]; then
    ./kafka-topics.sh --topic ${KAFKA_TOPIC_NETFLOW_CSV} --delete --bootstrap-server ${KAFKA_BROKER_IP}:${KAFKA_BROKER_PORT}
fi
./kafka-topics.sh --topic ${KAFKA_TOPIC_AD_RESULTS} --delete --bootstrap-server ${KAFKA_BROKER_IP}:${KAFKA_BROKER_PORT}

if [[ "$1" != "AD_RESULTS_ONLY" ]]; then
    ./kafka-topics.sh --topic ${KAFKA_TOPIC_NETFLOW_CSV} --create --bootstrap-server ${KAFKA_BROKER_IP}:${KAFKA_BROKER_PORT} --partitions 1 --replication-factor 1
fi
./kafka-topics.sh --topic ${KAFKA_TOPIC_AD_RESULTS} --create --bootstrap-server ${KAFKA_BROKER_IP}:${KAFKA_BROKER_PORT} --partitions 1 --replication-factor 1

cd ${DIR}
