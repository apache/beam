#!/bin/sh

# Install python
sudo apt-get update
sudo apt-get install python-pip
sudo pip install --upgrade pip
sudo apt-get install python-dev
sudo pip install tornado numpy
echo

# Identify external IP of the pod
external_ip="$(kubectl get svc elasticsearch -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
echo "External IP - $external_ip"
echo

#run the script
/usr/bin/python es_test_data.py --es_url=http://$external_ip:9200
echo "Test data for Elasticsearch generated"

