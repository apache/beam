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

# Identify the pod
cassandra_pods="kubectl get pods -l name=cassandra"
running_seed="$(kubectl get pods -o json -l name=cassandra -o jsonpath='{.items[0].metadata.name}')"
echo "$running_seed"
cassandra_svc="kubectl get svc cassandra"
echo

# After starting the service, it takes couple of minutes to generate the external IP for the service. Hence, wait for sometime.
#sleep 2m

# Identify external IP of the pod
external_ip="$(kubectl get svc cassandra -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc cassandra -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"
done
echo "External IP - $external_ip"
echo

# Create keyspace
keyspace_creation_command="drop keyspace if exists ycsb;create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };"
kubectl exec -ti $running_seed -- cqlsh -e "$keyspace_creation_command"
echo "Keyspace created successfully."
echo "------------------------------"
echo "$keyspace_creation_command"
echo

# Create table
table_creation_command="use ycsb;drop table if exists usertable;create table usertable (y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar,field4 varchar,field5 varchar,field6 varchar,field7 varchar,field8 varchar,field9 varchar,field10 varchar,field11 varchar,field12 varchar,field13 varchar,field14 varchar,field15 varchar,field16 varchar);"
kubectl exec -ti $running_seed -- cqlsh -e "$table_creation_command"
echo "Table created successfully."
echo "-------------------------------"
echo "$table_creation_command"

cd ycsb-0.12.0

echo "Starting to load data"
echo "-----------------------------"
./bin/ycsb load cassandra-cql -p hosts=${external_ip} -P workloads/workloadd -s > workloada_load_res.txt
echo "Data loaded on ${external_ip} successfully."
