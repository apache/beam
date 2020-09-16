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
#  1. Create a new virtualenv and install the SDK
#  2. Run UserScore examples with DirectRunner
#  3. Run UserScore examples with DataflowRunner
#  4. Run HourlyTeamScore on DirectRunner
#  5. Run HourlyTeamScore on DataflowRunner
#

set -e
set -v

source release/src/main/python-release/python_release_automation_utils.sh

# Assign default values
BEAM_PYTHON_SDK=$BEAM_PYTHON_SDK_ZIP


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
# Run UserScore with DirectRunner
# Globals:
#   USERSCORE_OUTPUT_PREFIX, DATASET, BUCKET_NAME
# Arguments:
#   None
#######################################
function verify_userscore_direct() {
  print_separator "Running userscore example with DirectRunner"
  output_file_name="$USERSCORE_OUTPUT_PREFIX-direct-runner.txt"
  python -m apache_beam.examples.complete.game.user_score \
    --output=$output_file_name \
    --project=$PROJECT_ID \
    --dataset=$DATASET \
    --input=$GAME_INPUT_DATA

  verify_user_score "direct"
}


#######################################
# Run UserScore with DataflowRunner
# Globals:
#   USERSCORE_OUTPUT_PREFIX, PROJECT_ID, DATASET
#   BEAM_PYTHON_SDK, BUCKET_NAME
# Arguments:
#   None
#######################################
function verify_userscore_dataflow() {
  print_separator "Running userscore example with DataflowRunner"
  output_file_name="$USERSCORE_OUTPUT_PREFIX-dataflow-runner.txt"
  python -m apache_beam.examples.complete.game.user_score \
    --project=$PROJECT_ID \
    --region=$REGION_ID \
    --runner=DataflowRunner \
    --temp_location=gs://$BUCKET_NAME/temp/ \
    --sdk_location=$BEAM_PYTHON_SDK \
    --input=$GAME_INPUT_DATA \
    --output=gs://$BUCKET_NAME/$output_file_name

  verify_user_score "dataflow"
}


#######################################
# Run HourlyTeamScore with DirectRunner
# Globals:
#   PROJECT_ID, DATASET BUCKET_NAME
# Arguments:
#   None
#######################################
function verify_hourlyteamscore_direct() {
  print_separator "Running HourlyTeamScore example with DirectRunner"
  python -m apache_beam.examples.complete.game.hourly_team_score \
    --project=$PROJECT_ID \
    --dataset=$DATASET \
    --input=$GAME_INPUT_DATA \
    --temp_location=gs://$BUCKET_NAME/temp/ \
    --table="hourly_team_score_python_direct"

  verify_hourly_team_score "direct"
}


#######################################
# Run HourlyTeamScore with DataflowRunner
# Globals:
#   PROJECT_ID, DATASET
#   BEAM_PYTHON_SDK, BUCKET_NAME
# Arguments:
#   None
#######################################
function verify_hourlyteamscore_dataflow() {
  print_separator "Running HourlyTeamScore example with DataflowRunner"
  python -m apache_beam.examples.complete.game.hourly_team_score \
    --project=$PROJECT_ID \
    --region=$REGION_ID \
    --dataset=$DATASET \
    --runner=DataflowRunner \
    --temp_location=gs://$BUCKET_NAME/temp/ \
    --sdk_location $BEAM_PYTHON_SDK \
    --input=$GAME_INPUT_DATA \
    --table="hourly_team_score_python_dataflow"

  verify_hourly_team_score "dataflow"
}


#######################################
# Main function.
# This function validates Python RC MobileGaming in following steps:
#   1. Create a new virtualenv and install the SDK
#   2. Run UserScore examples with DirectRunner
#   3. Run UserScore examples with DataflowRunner
#   4. Run HourlyTeamScore on DirectRunner
#   5. Run HourlyTeamScore on DataflowRunner
# Globals:
#   VERSION
# Arguments:
#   $1 - sdk types: [tar, wheel]
#   $2 - python interpreter version: [python2.7, python3.5, ...]
#######################################
function run_release_candidate_python_mobile_gaming() {
  print_separator "Start Mobile Gaming Examples"
  echo "SDK version: $VERSION"

  TMPDIR=$(mktemp -d)
  echo $TMPDIR
  pushd $TMPDIR

  download_files $1 $2
  # get exact names of sdk and other files
  BEAM_PYTHON_SDK=$(get_sdk_name $1)

  install_sdk $1 $2
  verify_userscore_direct
  verify_userscore_dataflow
  verify_hourlyteamscore_direct
  verify_hourlyteamscore_dataflow

  complete "SUCCEED: Mobile Gaming Verification Complete"
}
