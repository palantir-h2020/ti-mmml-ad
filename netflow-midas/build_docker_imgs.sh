#!/bin/bash

set -o xtrace

cd midas_docker
docker build -t midas .
cd ..

cd alerts_consumer_docker
docker build -t alerts_consumer .
cd ..

docker images | grep "midas\|alerts_consumer"
