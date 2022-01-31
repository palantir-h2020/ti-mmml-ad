#!/bin/bash

echo "Waiting for Kafka..."
echo

(docker ps | grep -m 1 zookeeper) > /dev/null 2>&1 && ZOO=1 || ZOO=0
if [[ $ZOO -eq "1" ]]; then echo Zookeeper is ready; else echo Zookeeper is NOT ready; fi

(docker ps | grep -m 1 kafka) > /dev/null 2>&1 && KAF=1 || KAF=0
if [[ $KAF -eq "1" ]]; then echo Kafka is ready; else echo Kafka is NOT ready; fi

(docker exec -it kafka zookeeper-shell.sh zookeeper:2181 ls /brokers/ids | tail -n 1 | grep -m 1 "\[0\]") > /dev/null 2>&1 && KAF_BRO=1 || KAF_BRO=0
if [[ $KAF_BRO -eq "1" ]]; then echo Kafka broker is ready; else echo Kafka broker is NOT ready; fi

if [[ $ZOO -eq "0" ]]; then exit 1; fi

if [[ $KAF -eq "0" ]]; then exit 2; fi

if [[ $KAF_BRO -eq "0" ]]; then exit 3; fi

killall watch
