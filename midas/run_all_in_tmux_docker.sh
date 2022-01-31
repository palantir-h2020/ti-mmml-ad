#!/bin/bash

source common.sh

opt1="Test T5.2 alone (raw nfcapd CSV data directly ingested into '${KAFKA_TOPIC_NETFLOW_CSV}' topic) with a local Docker-based Kafka instance"
opt2="Test T5.2 alone (raw nfcapd CSV data directly ingested into '${KAFKA_TOPIC_NETFLOW_CSV}' topic)"
opt3="Test T5.1-T5.2 integration (raw nfcapd CSV data ingested into '${KAFKA_TOPIC_NETFLOW_RAW}' topic for pre-processing/anonymization and then sent into '${KAFKA_TOPIC_NETFLOW_ANONYM_PREPROC}' topic)"
PS3='Please enter your choice: '
options=("$opt1"  "$opt2" "$opt3" "Quit")
select opt in "${options[@]}"
do
    case $opt in
        "$opt1")
            option=1
            break
            ;;
        "$opt2")
            option=2
            break
            ;;
        "$opt3")
            option=3
            break
            ;;
        "Quit")
            exit
            ;;
        *) echo "invalid option $REPLY";;
    esac
done

./clean_docker.sh
rm /tmp/ready > /dev/null 2>&1

# Create new tmux session
tmux kill-session -t midas_session > /dev/null 2>&1
tmux new-session -d -s midas_session

# Split in 4 panels
tmux split-window -h
tmux select-layout even-horizontal
tmux split-window -v
tmux select-pane -t 0
tmux split-window -v

# Util commands
WAIT_KAFKA_CMD="clear; echo Waiting for Docker build and Kafka config...; while [ ! -f /tmp/ready ]; do sleep 1; done"

# Run commands in 4 panels
if [ "$option" -eq "1" ]; then
    # OPTION 1 (Test T5.2 alone, with local Kafka in Docker)
    tmux send-keys -t 0 "./build_docker_imgs_with_kafka.sh; ./create_kafka_topics_local_kafka.sh; touch /tmp/ready; clear; echo Press ENTER to ingest CSV events into '${KAFKA_TOPIC_NETFLOW_CSV}'; read; ./run_csv_events_ingest.sh ${KAFKA_TOPIC_NETFLOW_CSV} LOCAL_KAFKA" ENTER
    tmux send-keys -t 1 "${WAIT_KAFKA_CMD}; clear; ./run_midas_docker.sh ${KAFKA_TOPIC_NETFLOW_CSV} LOCAL_KAFKA" ENTER
    tmux send-keys -t 3 "${WAIT_KAFKA_CMD}; clear; ./add_midas_docker_instance.sh ${KAFKA_TOPIC_NETFLOW_CSV} LOCAL_KAFKA" ENTER
    tmux send-keys -t 2 "${WAIT_KAFKA_CMD}; clear; ./run_alerts_consumer_docker.sh LOCAL_KAFKA" ENTER
elif [ "$option" -eq "2" ]; then
    # OPTION 2 (Test T5.2 alone)
    tmux send-keys -t 0 "./build_docker_imgs.sh; ./create_kafka_topics.sh; touch /tmp/ready; clear; echo Press ENTER to ingest CSV events into '${KAFKA_TOPIC_NETFLOW_CSV}'; read; ./run_csv_events_ingest.sh ${KAFKA_TOPIC_NETFLOW_CSV}" ENTER
    tmux send-keys -t 1 "${WAIT_KAFKA_CMD}; clear; ./run_midas_docker.sh ${KAFKA_TOPIC_NETFLOW_CSV}" ENTER
    tmux send-keys -t 3 "${WAIT_KAFKA_CMD}; clear; ./add_midas_docker_instance.sh ${KAFKA_TOPIC_NETFLOW_CSV}" ENTER
    tmux send-keys -t 2 "${WAIT_KAFKA_CMD}; clear; ./run_alerts_consumer_docker.sh" ENTER
else
    # OPTION 3 (Test T5.1-T5.2 integration)
    # Swap the comments in the following two lines to skip nfcapd data ingest from this VM
    #tmux send-keys -t 0 "./build_docker_imgs.sh; ./create_kafka_topics.sh AD_RESULTS_ONLY; touch /tmp/ready; clear; echo Done!" ENTER
    tmux send-keys -t 0 "./build_docker_imgs.sh; ./create_kafka_topics.sh AD_RESULTS_ONLY; touch /tmp/ready; clear; echo Press ENTER to ingest CSV events into '${KAFKA_TOPIC_NETFLOW_RAW}'; read; ./run_csv_events_ingest.sh ${KAFKA_TOPIC_NETFLOW_RAW}" ENTER
    tmux send-keys -t 1 "${WAIT_KAFKA_CMD}; clear; ./run_midas_docker.sh ${KAFKA_TOPIC_NETFLOW_ANONYM_PREPROC}" ENTER
    tmux send-keys -t 3 "${WAIT_KAFKA_CMD}; clear; ./add_midas_docker_instance.sh ${KAFKA_TOPIC_NETFLOW_ANONYM_PREPROC}" ENTER
    tmux send-keys -t 2 "${WAIT_KAFKA_CMD}; clear; ./run_alerts_consumer_docker.sh" ENTER
fi
tmux select-pane -t 0
tmux attach-session -t midas_session
