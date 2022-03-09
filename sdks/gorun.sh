#!/bin/bash
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://MY-BUCKET/temp"
export REGION="us-central1"
export JOB_NAME="kafka-taxi-`date +%Y%m%d-%H%M%S`"
export BOOTSTRAP_SERVERS="123.45.67.89:1234"
export EXPANSION_ADDR="localhost:1234"
go run ./sdks/go/examples/kafka/taxi.go --expansion_addr=$EXPANSION_ADDR
#  --runner=DataflowRunner \
#  --temp_location=$TEMP_LOCATION \
#  --staging_location=$STAGING_LOCATION \
#  --project=$PROJECT \
#  --region=$REGION \
#  --job_name="${JOB_NAME}" \
#  --bootstrap_servers=$BOOTSTRAP_SERVER \
#  --experiments=use_portable_job_submission,use_runner_v2 \
