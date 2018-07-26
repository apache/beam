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
