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

PROJECT=apache-beam-testing

# Get first 50 instances.
CBT_INSTANCES=$(cbt -project="$PROJECT" listinstances | awk 'NR>2 {print $1} NR==52 {exit}')

CLEANUP_INSTANCE_TEMPLATES=(bt-read-tests bt-write-xlang bt-cdc-tests test[a-z]+)

# A grace period of 5 days.
GRACE_PERIOD=$(($(date +%s) - 24 * 3600 * 5))

# Count number of failed API calls.
declare -i failed_calls=0

# Disable change streams before deleting an instance.
disable_change_streams() {
  local instance=$1
  local tables
  local table

  tables=$(gcloud bigtable instances tables list \
    --instances="$instance" \
    --project="$PROJECT" \
    --format="value(name)") || return 1

  while IFS= read -r table; do
    [[ -z "$table" ]] && continue

    # Extract table ID from the full resource name.
    table=${table##*/}

    if ! gcloud bigtable instances tables update "$table" \
      --instance="$instance" \
      --project="$PROJECT" \
      --clear-change-stream-retention-period \
      --quiet; then
      echo "Failed to disable change stream for table $table in $instance"
      return 1
    fi
  done <<< "$tables"
}

for instance in $CBT_INSTANCES; do
  for template in "${CLEANUP_INSTANCE_TEMPLATES[@]}"; do
    pattern=$template-"([0-9]{8})"-
    if [[ $instance =~ $pattern ]]; then
      CREATE_DATE=${BASH_REMATCH[1]}
      if [[ $OSTYPE == "linux-gnu"* ]]; then
        # Skip if not a valid date.
        CREATED=$(date -d "$CREATE_DATE" +%s) || continue
      elif [[ $OSTYPE == "darwin"* ]]; then
        CREATED=$(date -ju -f "%Y%m%d-%H%M%S" "${CREATE_DATE}-000000" +%s) || continue
      else
        echo "Unsupported OS $OSTYPE"
        exit 1
      fi
      if [[ $GRACE_PERIOD -gt $CREATED ]]; then
        if disable_change_streams "$instance" &&
           cbt -project="$PROJECT" deleteinstance "$instance"; then
          echo "Deleted $instance (created $CREATE_DATE)"
        else
          echo "Failed to delete $instance"
          failed_calls+=1
        fi
      fi
      break
    fi
  done
done

# fail the script if failed_calls is nonzero
if [[ failed_calls -ne 0 ]]; then
  echo "Failed delete $failed_calls instances"
  exit 1
fi
