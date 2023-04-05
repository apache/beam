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
#
#    The Chicago Taxi example demonstrates the end-to-end workflow and steps
#    of how to analyze, validate and transform data, train a model, analyze
#    and serve it.
#
#    Example usage:
#    ./run_chicago.sh gs://my-gcs-bucket DataflowRunner \
#                     --sdk_location=\"apache-beam.tar.gz\" --region=\"us-central1\"
#
set -eo pipefail

if [[ -z "$1" ]]; then
  echo "GCS bucket name required"
  exit 1
fi

if [[ -z "$2" ]]; then
  echo "Runner required"
  exit 1
fi

GCS_BUCKET=$1
RUNNER=$2
PIPELINE_OPTIONS=$3

if [[ "$RUNNER" == "PortableRunner" ]]; then
  METRICS_TABLE_SUFFIX='_flink'
fi

# Loop through pipeline options and append
shift
while [[ $# -gt 2 ]]
do
  PIPELINE_OPTIONS=${PIPELINE_OPTIONS}" "$3
  shift
done

JOB_ID="chicago-taxi-tfdv-$(date +%Y%m%d-%H%M%S)"
JOB_OUTPUT_PATH=${GCS_BUCKET}/${JOB_ID}/chicago_taxi_output
TEMP_PATH=${GCS_BUCKET}/${JOB_ID}/tmp/
GCP_PROJECT=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
MAX_ROWS=100000
JOB_OUTPUT_PATH=${GCS_BUCKET}/${JOB_ID}/chicago_taxi_output
TFT_OUTPUT_PATH=${JOB_OUTPUT_PATH}/tft_output
EVAL_RESULT_DIR=${TFT_OUTPUT_PATH}/eval_result_dir


# Variables needed for subsequent stages.
TFDV_OUTPUT_PATH=${JOB_OUTPUT_PATH}/tfdv_output
SCHEMA_PATH=${TFDV_OUTPUT_PATH}/schema.pbtxt

echo Using GCP project: ${GCP_PROJECT}
echo Job output path: ${JOB_OUTPUT_PATH}
echo TFDV output path: ${TFDV_OUTPUT_PATH}


# Analyze and validate
# Compute stats and generate a schema based on the stats.
echo Starting distributed TFDV stats computation and schema generation...

python tfdv_analyze_and_validate.py \
  --input bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --infer_schema \
  --stats_path ${TFDV_OUTPUT_PATH}/train_stats.tfrecord \
  --schema_path ${SCHEMA_PATH} \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${TEMP_PATH} \
  --job_name ${JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='beam_performance' \
  --metrics_table='tfdv_analyze'${METRICS_TABLE_SUFFIX} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py \
  ${PIPELINE_OPTIONS}

EVAL_JOB_ID=${JOB_ID}-eval

# Compute stats for eval data and validate stats against the schema.
python tfdv_analyze_and_validate.py \
  --input bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --for_eval \
  --schema_path ${SCHEMA_PATH} \
  --validate_stats \
  --stats_path ${TFDV_OUTPUT_PATH}/eval_stats.tfrecord \
  --anomalies_path ${TFDV_OUTPUT_PATH}/anomalies.pbtxt \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${TEMP_PATH} \
  --job_name ${EVAL_JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='beam_performance' \
  --metrics_table='chicago_taxi_tfdv_validate'${METRICS_TABLE_SUFFIX} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py \
  ${PIPELINE_OPTIONS}

# End analyze and validate
echo Preprocessing train data...

python preprocess.py \
  --output_dir ${TFT_OUTPUT_PATH} \
  --outfile_prefix train_transformed \
  --input bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --schema_file ${SCHEMA_PATH} \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${TEMP_PATH} \
  --job_name ${JOB_ID} \
  --runner ${RUNNER} \
  --max_rows ${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='beam_performance' \
  --metrics_table='chicago_taxi_preprocess'${METRICS_TABLE_SUFFIX} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py \
  ${PIPELINE_OPTIONS}

#Train ML engine
TRAINER_JOB_ID="chicago_taxi_trainer_$(date +%Y%m%d_%H%M%S)"
TRAIN_OUTPUT_PATH=${JOB_OUTPUT_PATH}/trainer_output
WORKING_DIR=${TRAIN_OUTPUT_PATH}/working_dir

MODEL_DIR=${TRAIN_OUTPUT_PATH}/model_dir
# Inputs
TRAIN_FILE=${TFT_OUTPUT_PATH}/train_transformed-*
TF_VERSION=1.14
#workaround for boto in virtualenv, required for the gsutil commands to work:
export BOTO_CONFIG=/dev/null
# Start clean, but don't fail if the path does not exist yet.
gsutil rm ${TRAIN_OUTPUT_PATH} || true
# Options
TRAIN_STEPS=10000
EVAL_STEPS=1000
# Force a small eval so that the Estimator.train_and_eval() can be used to
# save the model with its standard paths.
EVAL_FILE=${TFT_OUTPUT_PATH}/train_transformed-*
echo Training the model
gcloud ml-engine jobs submit training ${TRAINER_JOB_ID} \
                                    --stream-logs \
                                    --job-dir ${MODEL_DIR} \
                                    --runtime-version ${TF_VERSION} \
                                    --module-name trainer.task \
                                    --package-path trainer/ \
                                    --region us-central1 \
                                    -- \
                                    --train-files ${TRAIN_FILE} \
                                    --train-steps ${TRAIN_STEPS} \
                                    --eval-files ${EVAL_FILE} \
                                    --eval-steps ${EVAL_STEPS} \
                                    --output-dir ${WORKING_DIR} \
                                    --schema-file ${SCHEMA_PATH} \
                                    --tf-transform-dir ${TFT_OUTPUT_PATH}

# We evaluate with the last eval model written (hence tail -n1)
EVAL_MODEL_DIR=${TRAIN_OUTPUT_PATH}/working_dir/eval_model_dir
LAST_EVAL_MODEL_DIR=$(gsutil ls ${EVAL_MODEL_DIR} | tail -n1)

echo Eval model dir: ${EVAL_MODEL_DIR}

python process_tfma.py \
  --big_query_table bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --schema_file ${SCHEMA_PATH} \
  --eval_model_dir ${LAST_EVAL_MODEL_DIR} \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${GCS_BUCKET}/${JOB_ID}/tmp/ \
  --job_name ${JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_eval_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='beam_performance' \
  --metrics_table='chicago_taxi_process_tfma'${METRICS_TABLE_SUFFIX} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py \
  ${PIPELINE_OPTIONS}
