#!/bin/bash

set -o xtrace

source common.sh

DIR=$(pwd)

# Topic deletion might not work when Kafka property delete.topic.enable is set to true.
# It seems --delete removes at least all the events from the topic.
docker exec -it kafka kafka-topics.sh --topic ${KAFKA_TOPIC_NETFLOW_CSV} --delete --zookeeper zookeeper:2181
docker exec -it kafka kafka-topics.sh --topic ${KAFKA_TOPIC_AD_RESULTS} --delete --zookeeper zookeeper:2181

docker exec -it kafka kafka-topics.sh --topic ${KAFKA_TOPIC_NETFLOW_CSV} --create --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics.sh --topic ${KAFKA_TOPIC_AD_RESULTS} --create --zookeeper zookeeper:2181 --partitions 1 --replication-factor 1

cd ${DIR}
