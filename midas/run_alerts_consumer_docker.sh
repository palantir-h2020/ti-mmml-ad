#!/bin/bash

set -o xtrace

source common.sh

if [[ "$1" != "LOCAL_KAFKA" ]]; then
    docker run --env KAFKA_BROKER_IP=${KAFKA_BROKER_IP} --env KAFKA_BROKER_PORT=${KAFKA_BROKER_PORT} --env KAFKA_TOPIC=${KAFKA_TOPIC_AD_RESULTS} --name=alerts_consumer alerts_consumer
else
    docker run --network=kafka_docker_default --env KAFKA_BROKER_IP=kafka --env KAFKA_BROKER_PORT=${KAFKA_BROKER_PORT} --env KAFKA_TOPIC=${KAFKA_TOPIC_AD_RESULTS} --name=alerts_consumer alerts_consumer
fi
