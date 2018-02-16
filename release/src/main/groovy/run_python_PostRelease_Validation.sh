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

#  This file will verify Apache/Beam python SDK by following steps:
#
#  1. Download files from RC staging location
#  2. Verify hashes
#  3. Create a new virtualenv and install the SDK
#  4. Run Wordcount examples with DirectRunner
#  5. Run Wordcount examples with DataflowRunner
#  6. Run streaming wordcount on DirectRunner
#  7. Run streaming wordcount on DataflowRunner

set -e
set -v

print_separator() {
    echo "############################################################################"
    echo $1
    echo "############################################################################"
}

update_gcloud() {
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz --output gcloud.tar.gz
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
    sleep 25
}

run_pubsub_pull(){
    gcloud pubsub subscriptions pull --project=$STREAMING_PROJECT_ID $PUBSUB_SUBSCRIPTION --limit=100 --auto-ack
}

create_pubsub(){
    gcloud pubsub topics create --project=$STREAMING_PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics create --project=$STREAMING_PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions create --project=$STREAMING_PROJECT_ID $PUBSUB_SUBSCRIPTION --topic $PUBSUB_TOPIC2
}

cleanup_pubsub(){
    gcloud pubsub topics delete --project=$STREAMING_PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics delete --project=$STREAMING_PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions delete --project=$STREAMING_PROJECT_ID $PUBSUB_SUBSCRIPTION
}



# Python Release Candidate
VERSION="2.3.0"
CANDIDATE_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/"
SHA1_FILE_NAME="apache-beam-$VERSION-python.zip.sha1"
MD5_FILE_NAME="apache-beam-$VERSION-python.zip.md5"
BEAM_PYTHON_SDK="apache-beam-$VERSION-python.zip"
BEAM_PYTHON_RELEASE="apache-beam-$VERSION-source-release.zip"


# Cloud Configurations
# for local test
#PROJECT_ID='my-first-project-190318'
#BUCKET_NAME='yifan_auto_verification_test_bucket'
#TEMP_DIR='/temp'
#STREAMING_PROJECT_ID='google.com:dataflow-streaming'
#STREAMING_BUCKET_NAME='python-streaming-test'
#STREAMING_TEMP_DIR='/temp'

PROJECT_ID='apache-beam-testing'
BUCKET_NAME='temp-storage-for-release-validation-tests'
TEMP_DIR='/quickstart'
STREAMING_PROJECT_ID='apache-beam-testing'
STREAMING_BUCKET_NAME='temp-storage-for-release-validation-tests'
STREAMING_TEMP_DIR='/quickstart'
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
wget $CANDIDATE_URL$BEAM_PYTHON_SDK
wget $CANDIDATE_URL$BEAM_PYTHON_RELEASE


#
# 2. Verify sha1 and md5 hashes
#

print_separator "Checking sha1 and md5 hashes"
hash_check=$(sha1sum -c $SHA1_FILE_NAME | head -1 |awk '{print $2}')
if [[ "$hash_check" != "OK" ]]
then
  exit 1
fi

hash_check=$(md5sum -c $MD5_FILE_NAME | head -1 |awk '{print $2}')
if [[ "$hash_check" != "OK" ]]
then
  exit 1
fi


#
# 3. create a new virtualenv and install the SDK
#

print_separator "Creating new virtualenv and installing the SDK"

unzip $BEAM_PYTHON_RELEASE
cd apache-beam-$VERSION/sdks/python/
virtualenv temp_virtualenv
. temp_virtualenv/bin/activate && python setup.py sdist && pip install dist/apache-beam-$VERSION.tar.gz[gcp]
gcloud_version=$(gcloud --version | head -1 | awk '{print $4}')
if [[ "$gcloud_version" < "189" ]]
then
  update_gcloud
fi


#
# 4. Run wordcount with DirectRunner
#

print_separator "Running wordcount example with DirectRunner and verify results"
python -m apache_beam.examples.wordcount --output wordcount_direct.txt
file="wordcount_direct.txt-00000-of-00001"
if [ -f "$file" ]
then
	echo "$file found."
else
	echo "ERROR: $file not found."
	complete "failed when running wordcount example with DirectRunner"
	exit 1
fi
echo "SUCCEED: wordcount successfully run on DirectRunner."


#
# 5. Run wordcount with DataflowRunner
#

print_separator "Running wordcount example with DataflowRunner: "
python -m apache_beam.examples.wordcount \
    --output gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT \
    --staging_location gs://$BUCKET_NAME$TEMP_DIR \
    --temp_location gs://$BUCKET_NAME$TEMP_DIR \
    --runner DataflowRunner \
    --job_name wordcount \
    --project $PROJECT_ID \
    --num_workers $NUM_WORKERS \
    --sdk_location dist/apache-beam-$VERSION.tar.gz

# verify results.
gcs_pull_result=$(gsutil ls gs://$BUCKET_NAME)
for index in {0..3}
do
    if [[ $gcs_pull_result != *"gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT-0000$index-of-00004"* ]]
    then
        echo "ERROR: The wordcount example failed on DataflowRunner"
        complete "failed when running wordcount example with DataflowRunner"
        exit 1
    fi
done
# clean output files from GCS
gsutil rm gs://$BUCKET_NAME/$WORDCOUNT_OUTPUT-*
echo "SUCCEED: wordcount successfully run on DataflowRunner."


#
# 6. Run Streaming wordcount with DirectRunner
#

create_pubsub
print_separator "Running Streaming wordcount example with DirectRunner"

python -m apache_beam.examples.streaming_wordcount \
--input_topic projects/$STREAMING_PROJECT_ID/topics/$PUBSUB_TOPIC1 \
--output_topic projects/$STREAMING_PROJECT_ID/topics/$PUBSUB_TOPIC2 \
--streaming &
pid=$!

sleep 15
# verify result
run_pubsub_publish
pull_result=$(run_pubsub_pull)
should_see="like: 1"
if [[ $pull_result = *"$should_see"* ]]
then
    echo "SUCCEED: The streaming wordcount example running successfully on DirectRunner."
else
    echo "ERROR: The streaming wordcount example failed on DirectRunner."
    cleanup_pubsub
    kill -9 $pid
    complete "failed when running streaming wordcount example with DirectRunner."
    exit 1
fi
kill -9 $pid
sleep 10

#
# 7. Run Streaming Wordcount with DataflowRunner
#

print_separator "Running Streaming wordcount example with DirectRunner "
python -m apache_beam.examples.streaming_wordcount \
    --streaming \
    --job_name pyflow-wordstream-candidate \
    --project $STREAMING_PROJECT_ID \
    --runner DataflowRunner \
    --input_topic projects/$STREAMING_PROJECT_ID/topics/$PUBSUB_TOPIC1 \
    --output_topic projects/$STREAMING_PROJECT_ID/topics/$PUBSUB_TOPIC2 \
    --staging_location gs://$STREAMING_BUCKET_NAME$STREAMING_TEMP_DIR \
    --temp_location gs://$STREAMING_BUCKET_NAME$STREAMING_TEMP_DIR \
    --num_workers $NUM_WORKERS \
    --sdk_location dist/apache-beam-$VERSION.tar.gz &

pid=$!


running_job=$(gcloud dataflow jobs list | grep pyflow-wordstream-candidate | grep Running | cut -d' ' -f1)

# verify result
run_pubsub_publish
pull_result=$(run_pubsub_pull)
echo $pull_result
if [[ $pull_result = *"$should_see"* ]]
then
    echo "SUCCEED: The streaming wordcount example running successfully on DataflowRunner."
else
    echo "ERROR: The streaming wordcount example failed on DataflowRunner."
    cleanup_pubsub
    kill -9 $pid
    gcloud dataflow jobs cancel $running_job
    complete "failed when running streaming wordcount example with DataflowRunner."
    exit 1
fi
kill -9 $pid
gcloud dataflow jobs cancel $running_job
cleanup_pubsub

complete "Succeed"
