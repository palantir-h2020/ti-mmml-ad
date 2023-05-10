#!/usr/bin/env bash

TENANT_ID="ANY_TENANT"
# TENANT_ID="1"

BASE_DIR="$(dirname "$(pwd)")"
MIDAS_DIR="${BASE_DIR}/k8s/midas_AD/midas_docker"

cd ${MIDAS_DIR}

echo "Rebuilding MIDAS docker image..."
docker build -t palantir-midas:1.0 . && docker tag palantir-midas:1.0 10.101.10.244:5000/palantir-midas:1.0 && docker push 10.101.10.244:5000/palantir-midas:1.0
if [[ $(kubectl get pods --all-namespaces | grep midas | wc -l) -gt 0 ]]; then
  echo "[!] Existing MIDAS pods found"
  ../../midas_k8s_clean_pods.sh
fi

echo "Creating MIDAS pod"
if [[ "$TENANT_ID" == "ANY_TENANT" ]]; then
  # Use all the partitions
  kubectl create -f ${MIDAS_DIR}/pod.yaml
else
  # Use only the partition assigned to a specific tenant
  sed -e 's|ANY_TENANT|'"${TENANT_ID}"'|g' -e 's|midas-pod|midas-pod-'"${TENANT_ID}"'|g' ${MIDAS_DIR}/pod.yaml | kubectl create -f -
fi

if [[ "$TENANT_ID" == "ANY_TENANT" ]]; then
  POD_NAME="midas-pod"
else
  POD_NAME="midas-pod-${TENANT_ID}"
fi

echo "Waiting for MIDAS pod startup"
while [[ $(kubectl get pods --all-namespaces | grep ${POD_NAME} | grep Running | wc -l) -eq 0 ]]; do
  echo -n "."
done
echo
echo "MIDAS pod started, attaching..."
kubectl logs ${POD_NAME} && kubectl attach ${POD_NAME}