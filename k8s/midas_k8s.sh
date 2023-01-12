#!/usr/bin/env bash

BASE_DIR="/media/palantir-nfs/ti-mmml-ad"
MIDAS_DIR="${BASE_DIR}/k8s/midas_AD/midas_docker"

echo "Rebuilding MIDAS docker image..."
cd ${MIDAS_DIR} && docker build -t palantir-midas:1.0 . && docker tag palantir-midas:1.0 10.101.10.244:5000/palantir-midas:1.0 && docker push 10.101.10.244:5000/palantir-midas:1.0
if [[ $(kubectl get pods --all-namespaces | grep midas | wc -l) -gt 0 ]]; then
  echo "Existing MIDAS pod found, deleting..."
  kubectl delete pod midas
fi

echo "Creating MIDAS pod"
kubectl create -f ${MIDAS_DIR}/pod.yaml

echo "Waiting for MIDAS pod startup"
while [[ $(kubectl get pods --all-namespaces | grep midas | grep Running | wc -l) -eq 0 ]]; do
  echo -n "."
done
echo
echo "MIDAS pod started, attaching..."
kubectl logs midas && kubectl attach midas
