#!/bin/sh

# Identify the pod
cassandra_pods="kubectl get pods -l name=cassandra1"
running_seed=$($cassandra_pods --no-headers | \
    grep Running | \
    grep 1/1 | \
    head -1 | \
    awk '{print $1}')
echo "$running_seed"
cassandra_svc="kubectl get svc cassandra1"
echo

sleep 2m

# Identify external IP of the pod
external_ip=$($cassandra_svc --no-headers | \
    awk '{print $3')
echo "External IP - $external_ip"
echo

# Create keyspace
keyspace_creation_command="drop keyspace if exists ycsb;create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };"
kubectl exec -ti $running_seed -- cqlsh -e "$keyspace_creation_command"
echo "Executed ----- $keyspace_creation_command"
echo

# Create table
table_creation_command="use ycsb;drop table if exists usertable;create table usertable (y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar,field4 varchar,field5 varchar,field6 varchar,field7 varchar,field8 varchar,field9 varchar,field10 varchar,field11 varchar,field12 varchar,field13 varchar,field14 varchar,field15 varchar,field16 varchar);"
kubectl exec -ti $running_seed -- cqlsh -e "$table_creation_command"
echo "Executed ----- $table_creation_command"

# load YCSB tool
echo
echo
echo "Downloading YCSB tool"
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.12.0/ycsb-0.12.0.tar.gz
tar xfvz ycsb-0.12.0.tar.gz
wget https://www.slf4j.org/dist/slf4j-1.7.22.tar.gz
tar xfvz slf4j-1.7.22.tar.gz
cp slf4j-1.7.22/slf4j-simple-*.jar ycsb-0.12.0/lib/
cp slf4j-1.7.22/slf4j-api-*.jar ycsb-0.12.0/lib/
echo "YCSB tool loaded"
cd ycsb-0.12.0
echo "Starting to load data"
./bin/ycsb load cassandra-cql -p hosts=${external_ip} -P workloads/workloadd -s > workloada_load_res.txt
echo "Data loaded on ${external_ip} successfully."
