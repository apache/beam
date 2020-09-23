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
#   Writes releasing version to stdout.
#   e.g. __version__ = '2.5.0' => 2.5.0
#   e.g. __version__ = '2.6.0.dev' => 2.5.0
#######################################
function get_version() {
  version=$(awk '/__version__/{print $3}' sdks/python/apache_beam/version.py)
  version=$(echo $version | cut -c 2- | rev | cut -c 2- | rev)
  if [[ $version = *".dev"* ]]; then
    version=$(echo $version | rev | cut -d'.' -f2- | rev)
    IFS='.' read -r -a array <<< "$version"
    minor_version=$((${array[1]}-1))
    version="${array[0]}.$minor_version.${array[2]}"
  fi
  echo $version
}


#######################################
# Download files including SDK, SHA512 and ASC.
# Globals:
#   BEAM_PYTHON_SDK*
# Arguments:
#   $1 - SDK type: tar, wheel
#   $2 - python interpreter version: python3.7, python3.8, ...
#######################################
function download_files() {
  if [[ $1 = *"wheel"* ]]; then
    if [[ $2 == "python3.6" ]]; then
      BEAM_PYTHON_SDK_WHL="apache_beam-$VERSION*-cp36-cp36m-manylinux1_x86_64.whl"
    elif [[ $2 == "python3.7" ]]; then
      BEAM_PYTHON_SDK_WHL="apache_beam-$VERSION*-cp37-cp37m-manylinux1_x86_64.whl"
    elif [[ $2 == "python3.8" ]]; then
      BEAM_PYTHON_SDK_WHL="apache_beam-$VERSION*-cp38-cp38-manylinux1_x86_64.whl"
    elif [[ $2 == "python3.9" ]]; then
      BEAM_PYTHON_SDK_WHL="apache_beam-$VERSION*-cp39-cp39-manylinux1_x86_64.whl"
    else
      echo "Unable to determine a Beam wheel for interpreter version $2."
      exit 1
    fi

    wget -r -l2 --no-parent -nd -A "$BEAM_PYTHON_SDK_WHL*" $RC_STAGING_URL
  else
    BEAM_PYTHON_SDK_ZIP="apache-beam-$VERSION.zip"
    wget -r -l2 --no-parent -nd -A "$BEAM_PYTHON_SDK_ZIP*" $RC_STAGING_URL
  fi
}


#######################################
# Stdout python sdk name.
# Globals:
#   BEAM_PYTHON_SDK_ZIP
# Arguments:
#   $1 - SDK type: tar, wheel
#######################################
function get_sdk_name() {
  sdk_name=$BEAM_PYTHON_SDK_ZIP
  if [[ $1 = *"wheel"* ]]; then
    sdk_name=$(ls | grep "/*.whl$")
  fi
  echo $sdk_name
}


#######################################
# Stdout sha512 file name.
# Arguments:
#   $1 - SDK type: tar, wheel
#######################################
function get_sha512_name() {
  if [[ $1 = *"wheel"* ]]; then
    echo $(ls | grep "/*.whl.sha512$")
  else
    echo $(ls | grep "/*.zip.sha512$")
  fi
}


#######################################
# Stdout ASC file name.
# Arguments:
#   $1 - SDK type: tar, wheel
#######################################
function get_asc_name() {
  if [[ $1 = *"wheel"* ]]; then
    echo $(ls | grep "/*.whl.asc$")
  else
    echo $(ls | grep "/*.zip.asc$")
  fi
}


#######################################
# Create a new virtualenv and install the SDK
# Globals:
#   BEAM_PYTHON_SDK
# Arguments:
#   $1 - SDK type: tar, wheel
#   $2 - python interpreter version: [python3.7, python3.8, ...]
#######################################
function install_sdk() {
  sdk_file=$(get_sdk_name $1)
  print_separator "Creating new virtualenv with $2 interpreter and installing the SDK from $sdk_file."
  gsutil version -l
  rm -rf ./temp_virtualenv_${2}
  virtualenv temp_virtualenv_${2} -p $2
  . temp_virtualenv_${2}/bin/activate
  gcloud_version=$(gcloud --version | head -1 | awk '{print $4}')
  if [[ "$gcloud_version" < "189" ]]; then
    update_gcloud
  fi
  pip install google-compute-engine
  pip install $sdk_file[gcp]
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
  # Suppress error and pass quietly if topic/subscription not exists. We don't want the script
  # to be interrupted in this case.
  gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC1 2> /dev/null || true
  gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC2 2> /dev/null || true
  gcloud pubsub subscriptions delete --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION 2> /dev/null || true
}


#######################################
# Verify results of streaming_wordcount.
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


#######################################
# Verify results of user_score.
# Globals:
#   BUCKET_NAME
# Arguments:
#   $1: Runner - direct, dataflow
#######################################
function verify_user_score() {
  expected_output_file_name="$USERSCORE_OUTPUT_PREFIX-$1-runner.txt"
  actual_output_files=$(ls)
  if [[ $1 = *"dataflow"* ]]; then
    actual_output_files=$(gsutil ls gs://$BUCKET_NAME)
    expected_output_file_name="gs://$BUCKET_NAME/$expected_output_file_name"
  fi
  echo $actual_output_files
  if [[ $actual_output_files != *$expected_output_file_name* ]]
  then
    echo "ERROR: The userscore example failed on $1-runner".
    complete "failed when running userscore example with $1-runner."
    exit 1
  fi

  if [[ $1 = *"dataflow"* ]]; then
    gsutil rm $expected_output_file_name*
  fi
  echo "SUCCEED: user_score successfully run on $1-runner."
}


#######################################
# Verify results of hourly_team_score.
# Globals:
#   DATASET
# Arguments:
#   Runner - direct, dataflow
#######################################
function verify_hourly_team_score() {
  retry=3
  should_see='AntiqueBrassPlatypus'
  while(( $retry >= 0 )); do
    if [[ $retry > 0 ]]; then
      bq_pull_result=$(bq head -n 500 $DATASET.hourly_team_score_python_$1)
      if [[ $bq_pull_result = *"$should_see"* ]]; then
        echo "SUCCEED: hourly_team_score example successful run on $1-runner"
        break
      else
        retry=$(($retry-1))
        echo "Did not find team scores, retry left: $retry"
        sleep 15
      fi
    else
      echo "FAILED: HourlyTeamScore example failed running on $1-runner. \
        Did not found scores of team $should_see in $DATASET.leader_board"
      complete "FAILED"
      exit 1
    fi
  done
}


# Python RC configurations
VERSION=$(get_version)
RC_STAGING_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/python"

# Cloud Configurations
PROJECT_ID='apache-beam-testing'
REGION_ID='us-central1'
BUCKET_NAME='temp-storage-for-release-validation-tests/nightly-snapshot-validation'
TEMP_DIR='/tmp'
DATASET='beam_postrelease_mobile_gaming'
NUM_WORKERS=1

WORDCOUNT_OUTPUT='wordcount_direct.txt'
PUBSUB_TOPIC1='wordstream-python-topic-1'
PUBSUB_TOPIC2='wordstream-python-topic-2'
PUBSUB_SUBSCRIPTION='wordstream-python-sub2'

# Mobile Gaming Configurations
DATASET='beam_postrelease_mobile_gaming'
USERSCORE_OUTPUT_PREFIX='python-userscore_result'
GAME_INPUT_DATA='gs://dataflow-samples/game/5000_gaming_data.csv'
