#!/usr/bin/env bash
#
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
#    Deletes stale and old BQ datasets that are left after tests.
#

set -euo pipefail

# Clean up the stale kubernetes workload of given cluster

PROJECT=apache-beam-testing
LOCATION=us-central1-a
CLUSTER=io-datastores

function should_teardown() {
  if [[ $1 =~ ^([0-9]+)([a-z]) ]]; then
    local time_scale=${BASH_REMATCH[1]}
    local time_unit=${BASH_REMATCH[2]}
    # cutoff = 8 h
    if [ $time_unit == y ] || [ $time_unit == d ]; then
      return 0
    elif [ $time_unit == h ] && [ $time_scale -ge 8 ]; then
      return 0
    fi
  fi
  return 1
}

gcloud container clusters get-credentials io-datastores --zone us-central1-a --project apache-beam-testing

while read NAME STATUS AGE; do
  if [[ $NAME =~ ^beam-.+(test|-it) ]] && should_teardown $AGE; then
    kubectl delete namespace $NAME
  fi
done < <( kubectl get namespaces --context=gke_${PROJECT}_${LOCATION}_${CLUSTER} )
