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
MAX_RESULT=1500
BQ_DATASETS=`bq --project_id=$PROJECT ls --max_results=$MAX_RESULT | tail -n $MAX_RESULT | sed s/^[[:space:]]*/${PROJECT}:/`

CLEANUP_DATASET_TEMPLATES=(beam_bigquery_samples_ beam_temp_dataset_ FHIR_store_ bq_query_schema_update_options_16 bq_query_to_table_16 '\:(bq_read_all_|combine_per_key_examples|filter_examples|hourly_team_score_|leader_board_|leaderboard_|game_stats_|python_|temp_dataset)[a-z_]*[0-9a-f]{12,}$')

# A grace period of 5 days
GRACE_PERIOD=$((`date +%s` - 24 * 3600 * 5))
# count number of failed api calls
declare -i failed_calls=0

for dataset in ${BQ_DATASETS[@]}; do
  for template in ${CLEANUP_DATASET_TEMPLATES[@]}; do
    if [[ $dataset =~ $template ]]; then
      # The BQ API reports LAST MODIFIED TIME in miliseconds, while unix works in seconds since epoch
      # thus why we need to convert to seconds.

      failed=0
      ds=`bq --format=json --project_id=$PROJECT show $dataset` || failed=1
      if [[ $failed -eq 1 ]]; then
        echo "Could not find dataset $dataset - it may have already been deleted, skipping"
      else
        [[ $ds =~ \"lastModifiedTime\":\"([0-9]+)\" ]]
        LAST_MODIFIED_MS=${BASH_REMATCH[1]}
        LAST_MODIFIED=$(($LAST_MODIFIED_MS / 1000))
        if [[ $GRACE_PERIOD -gt $LAST_MODIFIED ]]; then
          if bq --project_id=$PROJECT rm -r -f $dataset; then
            if [[ $OSTYPE == "linux-gnu"* ]]; then
              # date command usage depending on OS
              echo "Deleted $dataset (modified `date -d @$LAST_MODIFIED`)"
            elif [[ $OSTYPE == "darwin"* ]]; then
              echo "Deleted $dataset (modified `date -r $LAST_MODIFIED`)"
            fi
          else
            echo "Tried and failed to delete $dataset"
            failed_calls+=1
          fi
        fi
      fi
      break
    fi
  done
done

# fail the script if failed_calls is nonzero
if [[ failed_calls -ne 0 ]]; then
  echo "Failed delete $failed_calls datasets"
  exit 1
fi
