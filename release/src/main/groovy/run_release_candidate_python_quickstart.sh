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

source release/src/main/groovy/python_release_automation_utils.sh

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
# Verify results of hourly_team_score.
# Globals:
#   DATASET
# Arguments:
#   $1 - runner type: DirectRunner, DataflowRunner
#   $2 - pid: the pid of running pipeline
#   $3 - running_job (DataflowRunner only): the job id of streaming pipeline running on DataflowRunner
#######################################
function verify_steaming_result() {
    retry=3
    should_see="Python: "
    while(( $retry > 0 )); do
        pull_result=$(run_pubsub_pull)
        if [[ $pull_result = *"$should_see"* ]]; then
            echo "SUCCEED: The streaming wordcount example running successfully on $1."
            break
        else
            if [[ $retry > 0 ]]; then
                retry=$(($retry-1))
                echo "retry left: $retry"
                sleep 15
            else
                echo "ERROR: The streaming wordcount example failed on $1."
                cleanup_pubsub
                kill -9 $2
                if [[ $1 = "DataflowRunner" ]]; then
                    gcloud dataflow jobs cancel $3
                fi
                complete "failed when running streaming wordcount example with $1."
                exit 1
            fi
        fi
    done
}

print_separator "Start Quickstarts Examples"
echo "SDK version: $VERSION"

TMPDIR=$(mktemp -d)
echo $TMPDIR
pushd $TMPDIR

#
# 1. Download files from RC staging location
#

wget $CANDIDATE_URL$SHA1_FILE_NAME
wget $CANDIDATE_URL$ASC_FILE_NAME
wget $CANDIDATE_URL$BEAM_PYTHON_SDK

#
# 2. Verify sha1, md5 hashes and gpg signature
#

print_separator "Checking sha1 and md5 hashes"
hash_check=$(sha1sum -c $SHA1_FILE_NAME | head -1 |awk '{print $2}')
if [[ "$hash_check" != "OK" ]]
then
  echo "ERROR: The sha1 hash doesn't match."
  complete "The sha1 hash doesn't match."
  exit 1
fi
echo "SUCCEED: Hashes verification completed."

wget https://dist.apache.org/repos/dist/dev/beam/KEYS
gpg --import KEYS
gpg --verify $ASC_FILE_NAME $BEAM_PYTHON_SDK


#
# 3. create a new virtualenv and install the SDK
#

print_separator "Creating new virtualenv and installing the SDK"
virtualenv temp_virtualenv
. temp_virtualenv/bin/activate
gcloud_version=$(gcloud --version | head -1 | awk '{print $4}')
if [[ "$gcloud_version" < "189" ]]; then
  update_gcloud
fi
pip install $BEAM_PYTHON_SDK[gcp]


#
# 4. Run wordcount with DirectRunner
#

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


#
# 5. Run wordcount with DataflowRunner
#

print_separator "Running wordcount example with DataflowRunner "
python -m apache_beam.examples.wordcount \
    --output gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT \
    --staging_location gs://$BUCKET_NAME$TEMP_DIR \
    --temp_location gs://$BUCKET_NAME$TEMP_DIR \
    --runner DataflowRunner \
    --job_name wordcount \
    --project $PROJECT_ID \
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


#
# 6. Run Streaming wordcount with DirectRunner
#

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

# Delete the pubsub topics and subscription before running the second job. Will recreate them in the second job.
cleanup_pubsub
kill -9 $pid
sleep 10


#
# 7. Run Streaming Wordcount with DataflowRunner
#

create_pubsub
print_separator "Running Streaming wordcount example with DataflowRunner "
python -m apache_beam.examples.streaming_wordcount \
    --streaming \
    --job_name pyflow-wordstream-candidate \
    --project $PROJECT_ID \
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
cleanup_pubsub

complete "SUCCEED: Quickstart Verification Complete"
