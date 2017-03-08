#!/bin/sh

kubectl create -f es-discovery-service.yaml
kubectl create -f es-service.yaml
kubectl create -f es-master_rc.yaml
# Wait until es-master_rc deployment is provisioned
sleep 2m
kubectl create -f es-client_rc.yaml
kubectl create -f es-data_rc.yaml

