#!/bin/sh
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# This script starts HBase cluster and hbase-external service that allows to reach cluster
# from developer's machine. Once the cluster is working, scripts waits till
# external cluster endpoint will be available. It prints out configuration line that
# should be added to /etc/hosts file in order to work with HBase cluster.
set -e

kubectl create -f hbase-single-node-cluster.yml
kubectl create -f hbase-single-node-cluster-for-local-dev.yml

external_ip="$(kubectl get svc hbase-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

echo "Waiting for the HBase service to come up ........"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc hbase-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
   echo "."
done

hbase_master_pod_name="$(kubectl get pods --selector=name=hbase -o jsonpath='{.items[*].metadata.name}')"
hbase_master_namespace="$(kubectl get pods --selector=name=hbase -o jsonpath='{.items[*].metadata.namespace}')"

echo "For local tests please add the following entry to /etc/hosts file"
printf "%s\\t%s.hbase.%s.svc.cluster.local\\n" "${external_ip}" "${hbase_master_pod_name}" "${hbase_master_namespace}"
