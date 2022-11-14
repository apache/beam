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
BQ_DATASETS=`bq --format=json --project_id=$PROJECT ls --max_results=1500 | jq -r .[].id`

CLEANUP_DATASET_TEMPLATES=(beam_bigquery_samples_ beam_temp_dataset_ FHIR_store_ bq_query_schema_update_options_16 bq_query_to_table_16 bq_read_all_[a-z0-9]*)

# A grace period of 5 days
GRACE_PERIOD=$((`date +%s` - 24 * 3600 * 5))

for dataset in ${BQ_DATASETS[@]}; do
  for template in ${CLEANUP_DATASET_TEMPLATES[@]}; do
    if [[ $dataset =~ $template ]]; then
      # The BQ API reports LAST MODIFIED TIME in miliseconds, while unix works in seconds since epoch
      # thus why we need to convert to seconds.
      LAST_MODIFIED_MS=`bq --format=json --project_id=$PROJECT show $dataset | jq -r .lastModifiedTime`
      LAST_MODIFIED=$(($LAST_MODIFIED_MS / 1000))
      if [[ $GRACE_PERIOD -gt $LAST_MODIFIED ]]; then
        echo "Deleting $dataset (modified `date -d @$LAST_MODIFIED`) Command bq --project_id=$PROJECT rm -r -f $dataset"
        bq --project_id=$PROJECT rm -r -f $dataset
      fi
    fi
  done
done
