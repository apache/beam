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

RELEASE=
REPO_URL=
RC_NUM=
RELEASE_BRANCH=
LOCAL_CLONE_DIR=rc_validations
BEAM_ROOT_DIR=beam
GIT_REPO_URL=https://github.com/apache/beam.git

echo "[Input Required] Please enter the release version: "
read RELEASE
RELEASE_BRANCH=release-${RELEASE}
echo "[Input Required] Please enter the release candidate number(1-3): "
read RC_NUM
echo "[Input Required] Please copy the repo URL from the vote email sent out by Release Manager:"
echo "The URL should look like: https://repository.apache.org/content/repositories/orgapachebeam-${KEY}"
read REPO_URL

echo "====================Checking Environment Variables================="
echo "running validations on release ${RELEASE} RC${RC_NUM}."
echo "repo URL for this RC: ${REPO_URL}"
echo "[Confirmation Required] Do you confirm all infos above are correct? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "====================Cloning Beam Release Branch===================="
cd ~
mkdir ${LOCAL_CLONE_DIR}
cd ${LOCAL_CLONE_DIR}
git clone ${GIT_REPO_URL}
cd ${BEAM_ROOT_DIR}
git checkout ${RELEASE_BRANCH}

echo "====================Starting Java Quickstart======================="

echo "[Current task] Java quickstart with direct runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-direct-java:runQuickstartJavaDirect \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Apex local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-apex:runQuickstartJavaApex \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Flink local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-flink_2.11:runQuickstartJavaFlinkLocal \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Spark local runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  ./gradlew :beam-runners-spark:runQuickstartJavaSpark \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE}
fi

echo "[Current task] Java quickstart with Dataflow runner"
echo "[Confirmation Required] Do you want to start this task? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[GCP Project Required] Please input your GCP project:"
  read USER_GCP_PROJECT
  echo "[GCP GCS Bucket Required] Please input your GCS bucket: "
  read USER_GCS_BUCKET
  ./gradlew :beam-runners-google-cloud-dataflow-java:runQuickstartJavaDataflow \
  -Prepourl=${REPO_URL} \
  -Pver=${RELEASE} \
  -PgcpProject=${USER_GCP_PROJECT} \
  -PgcsBucket=${USER_GCS_BUCKET}
fi

echo "===================Starting Java Mobile Game====================="
echo "[GCP Project Required] Please input your GCP project:"
read USER_GCP_PROJECT
MOBILE_GAME_DATASET=${USER}_${RELEASE}_java_validations
MOBILE_GAME_PUBSUB_TOPIC=leader_board-${USER}-${RELEASE}-java-topic-1
echo "Please review following GCP sources setup: "
echo "Using GCP project: ${USER_GCP_PROJECT}"
echo "Will create BigQuery dataset: ${MOBILE_GAME_DATASET}"
echo "Will create Pubsub topic: ${MOBILE_GMAE_PUBSUB_TOPIC}"
echo "[Confirmation Required] Do you want to run validations with configurations above? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "-----------------Setting Up Service Account------------------------"
  echo "Please go to GCP IAM console under your project(${USER_GCP_PROJECT})."
  echo "Create a service account as project owner."
  echo "[Input Required] Please enter your service account name:"
  read USER_SERVICE_ACCOUNT_NAME
  SERVICE_ACCOUNT_KEY_JSON=${USER}_json_key.json
  gcloud iam service-accounts keys create ${SERVICE_ACCOUNT_KEY_JSON} --iam-account ${USER_SERVICE_ACCOUNT_NAME}@${USER_GCP_PROJECT}
  export GOOGLE_APPLICATION_CREDENTIALS=pwd/${SERVICE_ACCOUNT_KEY_JSON}

  echo "-------------------Creating BigQuery Dataset-----------------------"
  bq mk --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}

  echo "----------------------Creating Pubsub Topic------------------------"
  gcloud alpha pubsub topics create --project=${USER_GCP_PROJECT} ${MOBILE_GAME_PUBSUB_TOPIC}

  echo "[Current task] Java mobile game validations: UserScore, HourlyTeamScore, Leaderboard"
  ./gradlew :beam-runners-google-cloud-dataflow-java:runMobileGamingJavaDataflow \
  -Prepourl=${REPO_URL} \
  -Pver= ${RELEASE} \
  -PgcpProject=${USER_GCP_PROJECT} \
  -PgcsBucket=gs://dataflow-samples/game/gaming_data1.csv \
  -PbqDataset=${MOBILE_GAME_DATASET} -PpubsubTopic=${MOBILE_GAME_PUBSUB_TOPIC}

  echo "-------------------Cleaning Up BigQuery Dataset-------------------"
  bq rm -rf --project=${USER_GCP_PROJECT} ${MOBILE_GAME_DATASET}
fi






















