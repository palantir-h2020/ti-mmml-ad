#!/bin/bash

set -o xtrace

cd midas_docker
docker build -t midas .
cd ..

cd alerts_consumer_docker
docker build -t alerts_consumer .
cd ..

cd kafka_docker
docker-compose up -d
cd ..

docker images | grep "midas\|alerts_consumer\|kafka"

# Wait for kafka container to be ready
watch -n 5 ./kafka_docker_checker.sh
