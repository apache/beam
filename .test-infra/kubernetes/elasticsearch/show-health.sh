# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/sh
set -e

external_ip="$(kubectl get svc elasticsearch-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

echo "Waiting for the Elasticsearch service to come up ........"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc elasticsearch-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
   echo "."
done

echo "Elasticsearch cluster health info"
echo "---------------------------------"
curl $external_ip:9200/_cluster/health
echo # empty line since curl doesn't output CRLF
