#!/usr/bin/env bash

TENANT_ID="ANY_TENANT"
# TENANT_ID="1"

BASE_DIR="$(dirname "$(pwd)")"
AD_aggregator_DIR="${BASE_DIR}/k8s/AD_aggregator/ad_aggr_docker"

cd ${AD_aggregator_DIR}

echo "Rebuilding AD_aggregator docker image..."
docker build -t palantir-ad-aggregator:1.0 . && docker tag palantir-ad-aggregator:1.0 10.101.10.244:5000/palantir-ad-aggregator:1.0 && docker push 10.101.10.244:5000/palantir-ad-aggregator:1.0
if [[ $(kubectl get pods --all-namespaces | grep ad-aggregator | wc -l) -gt 0 ]]; then
  echo "[!] Existing AD_aggregator pods found"
  ../../ad_aggregator_k8s_clean_pods.sh
fi

echo "Creating AD_aggregator pod"
if [[ "$TENANT_ID" == "ANY_TENANT" ]]; then
  # Use all the partitions
  kubectl create -f ${AD_aggregator_DIR}/pod.yaml
else
  # Use only the partition assigned to a specific tenant
  sed -e 's|ANY_TENANT|'"${TENANT_ID}"'|g' -e 's|ad-aggregator-pod|ad-aggregator-pod-'"${TENANT_ID}"'|g' ${AD_aggregator_DIR}/pod.yaml | kubectl create -f -
fi

if [[ "$TENANT_ID" == "ANY_TENANT" ]]; then
  POD_NAME="ad-aggregator-pod"
else
  POD_NAME="ad-aggregator-pod-${TENANT_ID}"
fi

echo "Waiting for AD_aggregator pod startup"
while [[ $(kubectl get pods --all-namespaces | grep ${POD_NAME} | grep Running | wc -l) -eq 0 ]]; do
  echo -n "."
done
echo
echo "AD_aggregator pod started, attaching..."
kubectl logs ${POD_NAME} && kubectl attach ${POD_NAME}
