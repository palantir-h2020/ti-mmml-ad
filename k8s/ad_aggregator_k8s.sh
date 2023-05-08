#!/usr/bin/env bash

BASE_DIR="/media/palantir-nfs/ti-mmml-ad"
AD_aggregator_DIR="${BASE_DIR}/k8s/AD_aggregator/ad_aggr_docker"

echo "Rebuilding AD_aggregator docker image..."
cd ${AD_aggregator_DIR} && docker build -t palantir-ad-aggregator:1.0 . && docker tag palantir-ad-aggregator:1.0 10.101.10.244:5000/palantir-ad-aggregator:1.0 && docker push 10.101.10.244:5000/palantir-ad-aggregator:1.0
if [[ $(kubectl get pods --all-namespaces | grep ad-aggregator | wc -l) -gt 0 ]]; then
  echo "Existing AD_aggregator pod found, deleting..."
  kubectl delete pod ad-aggregator
fi

echo "Creating AD_aggregator pod"
# Use any partition
kubectl create -f ${AD_aggregator_DIR}/pod.yaml
# Use only the partition assigned to tenant 1
# sed -e 's|ANY_TENANT|1|g' ${AD_aggregator_DIR}/pod.yaml | kubectl create -f -

echo "Waiting for AD_aggregator pod startup"
while [[ $(kubectl get pods --all-namespaces | grep ad-aggregator | grep Running | wc -l) -eq 0 ]]; do
  echo -n "."
done
echo
echo "AD_aggregator pod started, attaching..."
kubectl logs ad-aggregator && kubectl attach ad-aggregator
