#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Usage:
# With DirectRunner:
# ./batch_log_analysis.sh --runner DirectRunner \
#   --project YOUR_PROJECT \
#   --region YOUR_REGION \
#   --warehouse gs://YOUR-BUCKET \
#   --bq_table YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE

# With DataflowRunner:
# ./batch_log_analysis.sh --runner DataflowRunner \
#   --project YOUR_PROJECT \
#   --region YOUR_REGION \
#   --temp_location gs://YOUR-BUCKET/temp \
#   --num_workers 1 \
#   --worker_machine_type n1-standard-2 \
#   --warehouse gs://YOUR-BUCKET \
#   --bq_table YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE

set -e

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runner) RUNNER="$2"; shift 2 ;;
    --project) PROJECT="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --temp_location) TEMP_LOCATION="$2"; shift 2 ;;
    --num_workers) NUM_WORKERS="$2"; shift 2 ;;
    --worker_machine_type) WORKER_MACHINE_TYPE="$2"; shift 2 ;;
    --warehouse) WAREHOUSE="$2"; shift 2 ;;
    --bq_table) BQ_TABLE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ "$RUNNER" == "DataflowRunner" ]]; then
  DATAFLOW_COMMON_ARGS="--job_name batch-log-analysis-$(date +%Y%m%d-%H%M%S)
    --project $PROJECT --region $REGION
    --temp_location $TEMP_LOCATION
    --num_workers $NUM_WORKERS --worker_machine_type $WORKER_MACHINE_TYPE
    --disk_size_gb 50"
else
  DATAFLOW_COMMON_ARGS=""
fi

if ! command -v python &> /dev/null; then
  echo "Error: Python is not installed. Please install Python to continue."
  exit 1
fi

if ! command -v gcloud &> /dev/null; then
  echo "Error: gcloud CLI is not installed. Please install gcloud to continue."
  exit 1
fi

if [ -d "./beam-ml-artifacts" ]; then
  echo "Removing existing MLTransform's artifact directory..."
  rm -rf ./beam-ml-artifacts
fi

echo "Running iceberg_migration.yaml pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file iceberg_migration.yaml \
  --runner $RUNNER \
  --jinja_variables '{ "WAREHOUSE":"'$WAREHOUSE'", "PROJECT":"'$PROJECT'", "REGION":"'$REGION'" }' \
  $DATAFLOW_COMMON_ARGS

echo "Running ml_preprocessing.yaml pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file ml_preprocessing.yaml \
  --runner $RUNNER \
  --jinja_variables '{ "WAREHOUSE":"'$WAREHOUSE'", "PROJECT":"'$PROJECT'", "REGION":"'$REGION'", "BQ_TABLE":"'$BQ_TABLE'" }' \
  $DATAFLOW_COMMON_ARGS \
  --requirements_file requirements.txt

echo "Running train.py..."
python train.py --bq_table $BQ_TABLE

if [ ! -f "./knn_model.pkl" ]; then
  echo "Error: Model artifact 'knn_model.pkl' not found. Ensure model training is successful."
  exit 1
fi

echo "Uploading trained model to GCS..."
gcloud storage cp "./knn_model.pkl" "$WAREHOUSE/knn_model.pkl"

echo "Running anomaly_scoring.yaml pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file anomaly_scoring.yaml \
  --runner $RUNNER \
  --jinja_variables '{ "WAREHOUSE":"'$WAREHOUSE'", "PROJECT":"'$PROJECT'", "REGION":"'$REGION'", "BQ_TABLE":"'$BQ_TABLE'" }' \
  $DATAFLOW_COMMON_ARGS \
  --requirements_file requirements.txt

echo "All steps completed."
