#!/usr/bin/env bash
set -e
echo Starting distributed TFDV stats computation and schema generation...

if [[ -z "$1" ]]; then
  echo "GCS bucket name required"
  exit 1
fi

if [[ -z "$2" ]]; then
  echo "Runner required"
  exit 1
fi

if [[ -z "$3" ]]; then
  echo "SDK location needed"
  exit 1
fi

GCS_BUCKET=$1
RUNNER=$2
SDK_LOCATION=$3

JOB_ID="chicago-taxi-tfdv-$(date +%Y%m%d-%H%M%S)"
JOB_OUTPUT_PATH=${GCS_BUCKET}/${JOB_ID}/chicago_taxi_output
TEMP_PATH=${GCS_BUCKET}/${JOB_ID}/tmp/
GCP_PROJECT=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
MAX_ROWS=2000
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

python tfdv_analyze_and_validate.py \
  --input bigquery-public-data.chicago_taxi_trips.taxi_trips \
  --infer_schema \
  --stats_path ${TFDV_OUTPUT_PATH}/train_stats.tfrecord \
  --schema_path ${SCHEMA_PATH} \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${TEMP_PATH} \
  --experiments shuffle_mode=auto \
  --job_name ${JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='chicago_taxi_metrics' \
  --metrics_table='tfdv_analyze' \
  --metric_reporting_project ${GCP_PROJECT} \
  --sdk_location=${SDK_LOCATION} \
  --setup_file ./setup.py

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
  --experiments shuffle_mode=auto \
  --job_name ${EVAL_JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='chicago_taxi_metrics' \
  --metrics_table='tfdv_validate' \
  --sdk_location=${SDK_LOCATION} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py

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
  --experiments shuffle_mode=auto \
  --job_name ${JOB_ID} \
  --runner ${RUNNER} \
  --max_rows ${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='chicago_taxi_metrics' \
  --metrics_table='preprocess' \
  --sdk_location=${SDK_LOCATION} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py



#Train ML engine
TRAINER_JOB_ID="chicago_taxi_trainer_$(date +%Y%m%d_%H%M%S)"
TRAIN_OUTPUT_PATH=${JOB_OUTPUT_PATH}/trainer_output
WORKING_DIR=${TRAIN_OUTPUT_PATH}/working_dir

MODEL_DIR=${TRAIN_OUTPUT_PATH}/model_dir
# Inputs
TRAIN_FILE=${TFT_OUTPUT_PATH}/train_transformed-*
TF_VERSION=1.12
# Start clean, but don't fail if the path does not exist yet.
gsutil rm ${TRAIN_OUTPUT_PATH} || true
# Options
TRAIN_STEPS=10000
EVAL_STEPS=1000
# Force a small eval so that the Estimator.train_and_eval() can be used to
# save the model with its standard paths.
EVAL_FILE=${TFT_OUTPUT_PATH}/train_transformed-*
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
  --eval_result_dir ${EVAL_RESULT_DIR} \
  --project ${GCP_PROJECT} \
  --region us-central1 \
  --temp_location ${GCS_BUCKET}/${JOB_ID}/tmp/ \
  --experiments shuffle_mode=auto \
  --job_name ${JOB_ID} \
  --save_main_session \
  --runner ${RUNNER} \
  --max_eval_rows=${MAX_ROWS} \
  --publish_to_big_query=true \
  --metrics_dataset='chicago_taxi_metrics' \
  --metrics_table='process_tfma' \
  --sdk_location=${SDK_LOCATION} \
  --metric_reporting_project ${GCP_PROJECT} \
  --setup_file ./setup.py
