#!/bin/sh

kubectl create -f cassandra_service.yml
kubectl create -f cassandra_controller.yml
