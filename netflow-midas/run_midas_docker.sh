#!/bin/bash

set -o xtrace

source common.sh

if [ $# -eq 0 ]
  then
    echo "KAFKA_TOPIC_IN arg required"
    exit
fi

if [[ "$2" != "LOCAL_KAFKA" ]]; then
    docker run --env KAFKA_BROKER_IP=${KAFKA_BROKER_IP} --env KAFKA_BROKER_PORT=${KAFKA_BROKER_PORT} --env KAFKA_TOPIC_IN=$1 --env KAFKA_TOPIC_OUT=${KAFKA_TOPIC_AD_RESULTS} --env MIDAS_SLOT=${MIDAS_SLOT1} --env MIDAS_THR=${MIDAS_THR1} --name=midas midas
else
    docker run --network=kafka_docker_default --env KAFKA_BROKER_IP=kafka --env KAFKA_BROKER_PORT=${KAFKA_BROKER_PORT} --env KAFKA_TOPIC_IN=$1 --env KAFKA_TOPIC_OUT=${KAFKA_TOPIC_AD_RESULTS} --env MIDAS_SLOT=${MIDAS_SLOT1} --env MIDAS_THR=${MIDAS_THR1} --name=midas midas
fi
