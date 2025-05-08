#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Function to validate if a BigQuery table exists and has rows.
# Usage: validate_table <table_name> [retry_delay_seconds]
# Exits with 0 if validation succeeds, 1 otherwise.
# Requires GCP_PROJECT_ID and BQ_DATASET to be set in the environment.

validate_table() {
  local table_name=$1
  echo "DEBUG: ===== Starting validate_table for table: $table_name ====="
  # Ensure required env vars are set (GCP_PROJECT_ID, BQ_DATASET are inherited)
  if [[ -z "$GCP_PROJECT_ID" || -z "$BQ_DATASET" ]]; then
     echo "ERROR: GCP_PROJECT_ID and BQ_DATASET must be set in the environment."
     exit 1 # Exit script if env vars missing
  fi

  local full_table_id="${GCP_PROJECT_ID}.${BQ_DATASET}.${table_name}"
  local full_table_id_show="${GCP_PROJECT_ID}:${BQ_DATASET}.${table_name}"
  local count=""
  local exit_code=1
  local retries=10
  local delay=60 # Default seconds between retries

  # Allow overriding delay via second argument (optional)
  if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then
      delay=$2
      echo "DEBUG: Using custom retry delay: ${delay}s for table ${table_name}"
  else
      echo "DEBUG: Using default retry delay: ${delay}s for table ${table_name}"
  fi
  echo "DEBUG: Full table ID: ${full_table_id}, Max retries: ${retries}"

  for i in $(seq 1 $retries); do
    echo "DEBUG: Starting attempt $i/$retries..."
    local query_output

    echo "DEBUG: Executing: bq query --project_id=${GCP_PROJECT_ID} --use_legacy_sql=false --format=sparse --max_rows=1 \"SELECT COUNT(*) FROM \`${full_table_id}\`\""
    query_output=$(bq query --project_id=${GCP_PROJECT_ID} \
                     --use_legacy_sql=false \
                     --format=sparse \
                     --max_rows=1 \
                     "SELECT COUNT(*) FROM \`${full_table_id}\`" 2>&1)
    exit_code=$?

    echo "DEBUG: bq query exit code: $exit_code"
    echo "DEBUG: bq query raw output: [$query_output]"

    if [ $exit_code -eq 0 ]; then
        echo "DEBUG: bq query exited successfully (code 0)."
        count=$(echo "$query_output" | tail -n 1 | tr -d '[:space:]')
        echo "DEBUG: Processed count after removing whitespace (from last line): [$count]"
        if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -gt 0 ]; then
            echo "DEBUG: Count [$count] is a positive integer. Validation successful for this attempt."
            break # Success! Found non-zero rows
        else
            echo "DEBUG: Count [$count] is zero or not a positive integer."
            if [[ "$count" == "0" ]]; then
               echo "DEBUG: Explicit count of 0 received."
            fi
        fi
    else
        echo "DEBUG: bq query failed (exit code: $exit_code)."
        echo "DEBUG: Checking table existence with bq show..."
        if ! bq show --project_id=${GCP_PROJECT_ID} "${full_table_id_show}" > /dev/null 2>&1; then
          echo "DEBUG: Table ${full_table_id_show} appears not to exist (bq show failed)."
        else
          echo "DEBUG: Table ${full_table_id_show} appears to exist (bq show succeeded), but query failed."
        fi
    fi

    if [ $i -lt $retries ]; then
      echo "DEBUG: Validation condition not met on attempt $i. Retrying in $delay seconds..."
      sleep $delay
    else
      echo "DEBUG: Final attempt ($i) failed."
    fi
  done

echo "DEBUG: ===== Final validation check for table: $table_name ====="
  if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -gt 0 ]; then
    echo "SUCCESS: Table ${table_name} has ${count} rows. Final validation OK."
    echo "DEBUG: validate_table returning 0 (success)."
    return 0 # Indicate success
  else
    echo "ERROR: Failed to get a non-zero row count for table ${table_name} after $retries retries (Last exit code: $exit_code, Last processed count: '$count')."
    echo "DEBUG: validate_table returning 1 (failure)."
    return 1 # Indicate failure
  fi
}

# Allow the script to be sourced using "source ./script.sh"
# and then call the function directly: "validate_table my_table 30"
# If the script is executed directly, check if arguments are provided and call the function.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <table_name> [retry_delay_seconds]"
    echo "Requires GCP_PROJECT_ID and BQ_DATASET env vars."
    exit 1
  fi
  validate_table "$@"
fi
