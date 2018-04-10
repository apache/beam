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
# This script terminates hdfs cluster and hadoop-external service. It checks /etc/hosts file
# for any unneeded entries and notifies user about them.
#

#!/bin/sh
set -e

external_ip="$(kubectl get svc hadoop-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

hadoop_master_pod_name="$(kubectl get pods --selector=name=hadoop -o jsonpath='{.items[*].metadata.name}')"

kubectl delete -f hdfs-single-datanode-cluster.yml

kubectl delete -f hdfs-single-datanode-cluster-for-local-dev.yml

if grep "$external_ip\|$hadoop_master_pod_name" /etc/hosts ; then
    echo "Remove entry from /etc/hosts."
fi
