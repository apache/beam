#!/bin/bash
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

#  This file will verify Apache/Beam release candidate python by following steps:
#
#  1. Download files from RC staging location
#  2. Verify hashes
#  3. Create a new virtualenv and install the SDK
#  4. Run Wordcount examples with DirectRunner
#  5. Run Wordcount examples with DataflowRunner
#  6. Run streaming wordcount on DirectRunner
#  7. Run streaming wordcount on DataflowRunner
#

set -e
set -v

source release/src/main/python-release/python_release_automation_utils.sh

# Assign default values
BEAM_PYTHON_SDK=$BEAM_PYTHON_SDK_ZIP
ASC_FILE_NAME=$BEAM_PYTHON_SDK_ZIP".asc"
SHA512_FILE_NAME=$BEAM_PYTHON_SDK_ZIP".sha512"

# Cleanup Pubsub once script exits
trap cleanup_pubsub EXIT


#######################################
# Remove temp directory when complete.
# Globals:
#   TMPDIR
# Arguments:
#   None
#######################################
function complete() {
  print_separator "Validation $1"
  rm -rf $TMPDIR
}


#######################################
# Verify sha512 hash and gpg signature
# Globals:
#   ASC_FILE_NAME, SHA512_FILE_NAME, BEAM_PYTHON_SDK
# Arguments:
#   None
#######################################
function verify_hash() {
  print_separator "Checking sha512 hash and gpg signature"
  hash_check=$(sha512sum -c $SHA512_FILE_NAME | head -1 |awk '{print $2}')
  if [[ "$hash_check" != "OK" ]]
  then
    echo "ERROR: The sha512 hash doesn't match."
    complete "The sha512 hash doesn't match."
    exit 1
  fi
  echo "SUCCEED: Hashes verification completed."

  wget https://dist.apache.org/repos/dist/dev/beam/KEYS
  gpg --import KEYS
  gpg --verify $ASC_FILE_NAME $BEAM_PYTHON_SDK
  gsutil version -l
}


#######################################
# Run wordcount with DirectRunner
# Arguments:
#   None
#######################################
function verify_wordcount_direct() {
  print_separator "Running wordcount example with DirectRunner"
  python -m apache_beam.examples.wordcount --output wordcount_direct.txt
  if ls wordcount_direct.txt* 1> /dev/null 2>&1; then
	echo "Found output file(s):"
	ls wordcount_direct.txt*
  else
	echo "ERROR: output file not found."
	complete "failed when running wordcount example with DirectRunner."
	exit 1
  fi
  echo "SUCCEED: wordcount successfully run on DirectRunner."
}


#######################################
# Run wordcount with DataflowRunner
# Globals:
#   BUCKET_NAME, WORDCOUNT_OUTPUT, TEMP_DIR
#   PROJECT_ID, NUM_WORKERS, BEAM_PYTHON_SDK
# Arguments:
#   None
#######################################
function verify_wordcount_dataflow() {
  print_separator "Running wordcount example with DataflowRunner "
  python -m apache_beam.examples.wordcount \
    --output gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT \
    --staging_location gs://$BUCKET_NAME$TEMP_DIR \
    --temp_location gs://$BUCKET_NAME$TEMP_DIR \
    --runner DataflowRunner \
    --job_name wordcount \
    --project $PROJECT_ID \
    --region $REGION_ID \
    --num_workers $NUM_WORKERS \
    --sdk_location $BEAM_PYTHON_SDK

  # verify results.
  wordcount_output_in_gcs="gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT"
  gcs_pull_result=$(gsutil ls gs://$BUCKET_NAME)
  if [[ $gcs_pull_result != *$wordcount_output_in_gcs* ]]; then
    echo "ERROR: The wordcount example failed on DataflowRunner".
    complete "failed when running wordcount example with DataflowRunner."
    exit 1
  fi

  # clean output files from GCS
  gsutil rm gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT-*
  echo "SUCCEED: wordcount successfully run on DataflowRunner."
}


#######################################
# Run Streaming wordcount with DirectRunner
# Globals:
#   PROJECT_ID, PUBSUB_TOPIC1, PUBSUB_TOPIC2
# Arguments:
#   None
#######################################
function verify_streaming_wordcount_direct() {
  cleanup_pubsub
  create_pubsub
  print_separator "Running Streaming wordcount example with DirectRunner"
  python -m apache_beam.examples.streaming_wordcount \
  --input_topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC1 \
  --output_topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC2 \
  --streaming &
  pid=$!
  sleep 15

  # verify result
  run_pubsub_publish
  verify_steaming_result "DirectRunner" $pid

  kill -9 $pid
  sleep 10
}


#######################################
# Run Streaming Wordcount with DataflowRunner
# Globals:
#   PROJECT_ID, PUBSUB_TOPIC1, PUBSUB_TOPIC2
#   BUCKET_NAME, TEMP_DIR, NUM_WORKERS, BEAM_PYTHON_SDK
# Arguments:
#   None
#######################################
function verify_streaming_wordcount_dataflow() {
  cleanup_pubsub
  create_pubsub
  print_separator "Running Streaming wordcount example with DataflowRunner "
  python -m apache_beam.examples.streaming_wordcount \
    --streaming \
    --job_name pyflow-wordstream-candidate \
    --project $PROJECT_ID \
    --region $REGION_ID \
    --runner DataflowRunner \
    --input_topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC1 \
    --output_topic projects/$PROJECT_ID/topics/$PUBSUB_TOPIC2 \
    --staging_location gs://$BUCKET_NAME$TEMP_DIR \
    --temp_location gs://$BUCKET_NAME$TEMP_DIR \
    --num_workers $NUM_WORKERS \
    --sdk_location $BEAM_PYTHON_SDK &

  pid=$!
  sleep 60
  running_job=$(gcloud dataflow jobs list | grep pyflow-wordstream-candidate | grep Running | cut -d' ' -f1)

  # verify result
  run_pubsub_publish
  sleep 420
  verify_steaming_result "DataflowRunner" $pid $running_job

  kill -9 $pid
  gcloud dataflow jobs cancel $running_job
}


#######################################
# Main function.
# This function validates Python RC Quickstart in following steps:
#   1. Download files from RC staging location
#   2. Verify hashes
#   3. Create a new virtualenv and install the SDK
#   4. Run Wordcount examples with DirectRunner
#   5. Run Wordcount examples with DataflowRunner
#   6. Run streaming wordcount on DirectRunner
#   7. Run streaming wordcount on DataflowRunner
# Globals:
#   VERSION
# Arguments:
#   $1 - sdk types: [tar, wheel]
#   $2 - python interpreter version: [python2.7, python3.5, ...]
#######################################
function run_release_candidate_python_quickstart(){
  print_separator "Start Quickstarts Examples"
  echo "SDK version: $VERSION"
  TMPDIR=$(mktemp -d)
  echo $TMPDIR
  pushd $TMPDIR

  download_files $1 $2
  # get exact names of sdk and other files
  BEAM_PYTHON_SDK=$(get_sdk_name $1)
  ASC_FILE_NAME=$(get_asc_name $1)
  SHA512_FILE_NAME=$(get_sha512_name $1)

  verify_hash
  install_sdk $1 $2
  verify_wordcount_direct
  verify_wordcount_dataflow
  verify_streaming_wordcount_direct
  verify_streaming_wordcount_dataflow

  complete "SUCCEED: Quickstart Verification Complete"
}
