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


set -e
set -v

#######################################
# Print Separators.
# Arguments:
#   Info to be printed.
# Outputs:
#   Writes info to stdout.
#######################################
function print_separator() {
    echo "############################################################################"
    echo $1
    echo "############################################################################"
}

#######################################
# Update gcloud version.
# Arguments:
#   None
#######################################
function update_gcloud() {
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz \
    --output gcloud.tar.gz
    tar xf gcloud.tar.gz
    ./google-cloud-sdk/install.sh --quiet
    . ./google-cloud-sdk/path.bash.inc
    gcloud components update --quiet || echo 'gcloud components update failed'
    gcloud -v
}

#######################################
# Get Python SDK version from sdk/python/apache_beam/version.py.
# Arguments:
#   None
# Outputs:
#   Writes version to stdout.
#######################################
function get_version() {
    version=$(awk '/__version__/{print $3}' sdks/python/apache_beam/version.py)
    if [[ $version = *".dev"* ]]; then
        echo $version | cut -c 2- | rev | cut -d'.' -f2- | rev
    else
        echo $version
    fi
}

#######################################
# Publish data to Pubsub topic for streaming wordcount examples.
# Arguments:
#   None
#######################################
function run_pubsub_publish(){
    words=("hello world!", "I like cats!", "Python", "hello Python", "hello Python")
    for word in ${words[@]}; do
        gcloud pubsub topics publish $PUBSUB_TOPIC1 --message "$word"
    done
    sleep 10
}

#######################################
# Pull data from Pubsub.
# Arguments:
#   None
#######################################
function run_pubsub_pull() {
    gcloud pubsub subscriptions pull --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --limit=100 --auto-ack
}

#######################################
# Create Pubsub topics and subscription.
# Arguments:
#   None
#######################################
function create_pubsub() {
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions create --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --topic $PUBSUB_TOPIC2
}

#######################################
# Remove Pubsub topics and subscription.
# Arguments:
#   None
#######################################
function cleanup_pubsub() {
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions delete --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION
}


# Python RC configurations
VERSION=2.4.0
CANDIDATE_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/"
BEAM_PYTHON_SDK="apache-beam-$VERSION-python.zip"

# Cloud Configurations
PROJECT_ID='my-first-project-190318'
BUCKET_NAME='my-first-project-190318'
TEMP_DIR='/temp'
DATASET='beam_postrelease_mobile_gaming'
NUM_WORKERS=1

# Quickstart configurations
SHA1_FILE_NAME="apache-beam-$VERSION-python.zip.sha1"
ASC_FILE_NAME="apache-beam-$VERSION-python.zip.asc"

WORDCOUNT_OUTPUT='wordcount_direct.txt'
PUBSUB_TOPIC1='wordstream-python-topic-1'
PUBSUB_TOPIC2='wordstream-python-topic-2'
PUBSUB_SUBSCRIPTION='wordstream-python-sub2'

# Mobile Gaming Configurations
DATASET='beam_postrelease_mobile_gaming'
USERSCORE_OUTPUT_PREFIX='python-userscore_result'
