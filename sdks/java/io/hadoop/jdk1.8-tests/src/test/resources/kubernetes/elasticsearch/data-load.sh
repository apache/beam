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

#!/bin/bash
set -e

# Identify external IP
external_ip="$(kubectl get svc elasticsearch -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
echo "Waiting for the Elasticsearch service to come up ........"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc elasticsearch -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
   echo "."
done
echo "External IP - $external_ip"
echo

# Run the script
/usr/bin/python es_test_data.py --count=1000 --format=Txn_ID:int,Item_Code:int,Item_ID:int,User_Name:str,last_updated:ts,Price:int,Title:str,Description:str,Age:int,Item_Name:str,Item_Price:int,Availability:bool,Batch_Num:int,Last_Ordered:tstxt,City:text --es_url=http://$external_ip:9200 &