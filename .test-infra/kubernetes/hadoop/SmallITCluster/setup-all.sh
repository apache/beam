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
#
# This script starts hdfs cluster and hadoop-external service that allows to reach cluster
# from developer's machine. Once the cluster is working, scripts waits till
# external cluster endpoint will be available. It prints out configuration line that
# should be added to /etc/hosts file in order to work with hdfs cluster.

set -e

kubectl create -f hdfs-single-datanode-cluster.yml
kubectl create -f hdfs-single-datanode-cluster-for-local-dev.yml

external_ip="$(kubectl get svc hadoop-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

echo "Waiting for the Hadoop service to come up ........"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc hadoop-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
   echo "."
done

hadoop_master_pod_name="$(kubectl get pods --selector=name=hadoop -o jsonpath='{.items[*].metadata.name}')"

echo "For local tests please add the following entry to /etc/hosts file"
printf "%s\\t%s\\n" "${external_ip}" "${hadoop_master_pod_name}"

