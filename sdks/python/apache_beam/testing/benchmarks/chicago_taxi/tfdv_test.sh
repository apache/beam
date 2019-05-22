#!/usr/bin/env bash

if [[ -z "$MYBUCKET" ]]; then
  echo "\$MYBUCKET must be set."
  exit 1
fi

JOB_ID="chicago-taxi-tfdv-$(date +%Y%m%d-%H%M%S)"
JOB_OUTPUT_PATH=${MYBUCKET}/${JOB_ID}/chicago_taxi_output
TEMP_PATH=${MYBUCKET}/${JOB_ID}/tmp/
MYPROJECT=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
MAX_ROWS=2000
JOB_OUTPUT_PATH=${MYBUCKET}/${JOB_ID}/chicago_taxi_output
TFT_OUTPUT_PATH=${JOB_OUTPUT_PATH}/tft_output
EVAL_RESULT_DIR=${TFT_OUTPUT_PATH}/eval_result_dir


# Variables needed for subsequent stages.
TFDV_OUTPUT_PATH=${JOB_OUTPUT_PATH}/tfdv_output
SCHEMA_PATH=${TFDV_OUTPUT_PATH}/schema.pbtxt

echo Using GCP project: ${MYPROJECT}
echo Job output path: ${JOB_OUTPUT_PATH}
echo TFDV output path: ${TFDV_OUTPUT_PATH}


# Analyze and validate
# Compute stats and generate a schema based on the stats.

python tfdv_analyze_and_validate.py \
  --input bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --infer_schema \
  --stats_path ${TFDV_OUTPUT_PATH}/train_stats.tfrecord \
  --schema_path ${SCHEMA_PATH} \
  --project ${MYPROJECT} \
  --region us-central1 \
  --temp_location ${TEMP_PATH} \
  --experiments shuffle_mode=auto \
  --setup_file ./setup.py \
  --job_name ${JOB_ID} \
  --save_main_session \
  --runner DirectRunner \
  --max_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='tfdv_test' \
  --metrics_table='python_tfdv_test1' \
  --sdk_location='../../../../dist/apache-beam-2.14.0.dev0.tar.gz' \
  --metric_reporting_project ${MYPROJECT}