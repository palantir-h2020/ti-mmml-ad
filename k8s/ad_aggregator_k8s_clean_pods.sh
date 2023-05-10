#!/usr/bin/env bash

# kubectl get pods -n <namespace> --no-headers=true | awk '/application/{print $1}'| xargs  kubectl delete -n <namespace> pod
if [[ $(kubectl get pods --no-headers=true | grep ad-aggregator | wc -l) -eq 0 ]]; then
    echo "No ad-aggregator* pods found"
    exit
fi
echo "Removing the following ad-aggregator* pods:"
kubectl get pods --no-headers=true | grep ad-aggregator | awk '{print $1}'| xargs echo
read -p 'Press ENTER to continue: '
kubectl get pods --no-headers=true | grep ad-aggregator | awk '{print $1}'| xargs kubectl delete pod