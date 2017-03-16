#!/bin/sh

external_ip="$(kubectl get svc elasticsearch -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

echo "Elasticsearch cluster health info"
echo "---------------------------------"
curl $external_ip:9200/_cluster/health
echo # empty line since curl doesn't output CRLF
