#!/usr/bin/env bash

if [ -n "$1" ]; then
    POD_NAME="$1"
else
    echo "POD_NAME not provided!"
    exit
fi

if [[ $(kubectl get pods --no-headers=true --all-namespaces | grep ${POD_NAME} | wc -l) -eq 0 ]]; then
    echo "No ${POD_NAME} pods found"
    exit
fi
echo "Removing the following ${POD_NAME} pods:"
kubectl get pods --no-headers=true --all-namespaces | grep ${POD_NAME} | awk '{print $2}'| xargs echo
read -p 'Press ENTER to continue: '
NAMESPACE=$(kubectl get pods --no-headers=true --all-namespaces | grep ${POD_NAME} | awk '{print $1}')
kubectl get pods --no-headers=true --all-namespaces | grep ${POD_NAME} | awk '{print $2}'| xargs kubectl delete pod --namespace="${NAMESPACE}"