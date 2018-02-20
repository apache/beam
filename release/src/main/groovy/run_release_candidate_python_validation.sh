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

print_separator() {
    echo "############################################################################"
    echo $1
    echo "############################################################################"
}

get_version() {
    # this function will pull python sdk version from sdk/python/apache_beam/version.py and eliminate postfix '.dev'
    version=$(awk '/__version__/{print $3}' sdks/python/apache_beam/version.py)
    echo $version | cut -c 2- | rev | cut -d'.' -f2- | rev
}

update_gcloud() {
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz \
    --output gcloud.tar.gz
    tar xf gcloud.tar.gz
    ./google-cloud-sdk/install.sh --quiet
    . ./google-cloud-sdk/path.bash.inc
    gcloud components update --quiet || echo 'gcloud components update failed'
    gcloud -v
}

complete() {
    print_separator "Validation $1"
    rm -rf $TMPDIR
}

run_pubsub_publish(){
    words=("hello world!", "I like cats!", "Python", "hello Python", "hello Python")
    for word in ${words[@]}
    do
        gcloud pubsub topics publish $PUBSUB_TOPIC1 --message "$word"
    done
    sleep 10
}

run_pubsub_pull() {
    gcloud pubsub subscriptions pull --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --limit=100 --auto-ack
}

create_pubsub() {
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions create --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --topic $PUBSUB_TOPIC2
}

cleanup_pubsub() {
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions delete --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION
}

verify_steaming_result() {
    #  $1 - runner type: DirectRunner, DataflowRunner
    #  $2 - pid: the pid of running pipeline
    #  $3 - running_job (DataflowRunner only): the job id of streaming pipeline running on DataflowRunner
    retry=3
    should_see="Python: "
    while(( $retry > 0 ))
    do
        pull_result=$(run_pubsub_pull)
        if [[ $pull_result = *"$should_see"* ]]
        then
            echo "SUCCEED: The streaming wordcount example running successfully on $1."
            break
        else
            if [[ $retry > 0 ]]
            then
                ((retry-=1))
                echo "retry left: $retry"
                sleep 15
            else
                echo "ERROR: The streaming wordcount example failed on $1."
                cleanup_pubsub
                kill -9 $2
                if [[ $1 = "DataflowRunner" ]]
                then
                    gcloud dataflow jobs cancel $3
                fi
                complete "failed when running streaming wordcount example with $1."
                exit 1
            fi
        fi
    done
}

# Python Release Candidate Configuration
echo "SDK version: $(get_version)"
VERSION=$(get_version)
CANDIDATE_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/"
SHA1_FILE_NAME="apache-beam-$VERSION-python.zip.sha1"
MD5_FILE_NAME="apache-beam-$VERSION-python.zip.md5"
ASC_FILE_NAME="apache-beam-$VERSION-python.zip.asc"
BEAM_PYTHON_SDK="apache-beam-$VERSION-python.zip"

# Cloud Configurations
PROJECT_ID='apache-beam-testing'
BUCKET_NAME='temp-storage-for-release-validation-tests'
TEMP_DIR='/quickstart'
NUM_WORKERS=1
WORDCOUNT_OUTPUT='wordcount_direct.txt'
PUBSUB_TOPIC1='wordstream-python-topic-1'
PUBSUB_TOPIC2='wordstream-python-topic-2'
PUBSUB_SUBSCRIPTION='wordstream-python-sub2'

TMPDIR=$(mktemp -d)
echo $TMPDIR
pushd $TMPDIR

#
# 1. Download files from RC staging location
#
wget $CANDIDATE_URL$SHA1_FILE_NAME
wget $CANDIDATE_URL$MD5_FILE_NAME
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
hash_check=$(md5sum -c $MD5_FILE_NAME | head -1 |awk '{print $2}')
if [[ "$hash_check" != "OK" ]]
then
  echo "ERROR: The md5 hash doesn't match."
  complete "The md5 hash doesn't match."
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
. temp_virtualenv/bin/activate && pip install $BEAM_PYTHON_SDK[gcp]
gcloud_version=$(gcloud --version | head -1 | awk '{print $4}')
if [[ "$gcloud_version" < "189" ]]
then
  update_gcloud
fi


#
# 4. Run wordcount with DirectRunner
#

print_separator "Running wordcount example with DirectRunner"
python -m apache_beam.examples.wordcount --output wordcount_direct.txt
if ls wordcount_direct.txt* 1> /dev/null 2>&1;
then
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
if [[ $gcs_pull_result != *$wordcount_output_in_gcs* ]]
then
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

complete "Succeed"
