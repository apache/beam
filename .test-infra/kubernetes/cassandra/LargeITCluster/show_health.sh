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

# Output Cassandra cluster and pod status.

#!/bin/bash

find_cassandra_pods="kubectl get pods -l app=cassandra"

first_running_seed="$($find_cassandra_pods -o jsonpath="{.items[0].metadata.name}")"

# Use nodetool status command to determine the status of pods and display
cluster_status=$(kubectl exec $first_running_seed \
    -- /usr/local/apache-cassandra/bin/nodetool status -r)
echo
echo "  Cassandra Node      Kubernetes Pod"
echo "  --------------      --------------"
while read -r line; do
    node_name=$(echo $line | awk '{print $1}')
    status=$(echo "$cluster_status" | grep $node_name | awk '{print $1}')

    long_status=$(echo "$status" | \
        sed 's/U/  Up/g' | \
	sed 's/D/Down/g' | \
	sed 's/N/|Normal /g' | \
	sed 's/L/|Leaving/g' | \
	sed 's/J/|Joining/g' | \
	sed 's/M/|Moving /g')

    : ${long_status:="            "}
    echo "$long_status           $line"
done <<< "$($find_cassandra_pods)"

echo
