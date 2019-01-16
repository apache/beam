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

# This script will create a empty PR and start gradle publish job.

set -e

LOCAL_BEAM_DIR=beam_snapshot_build
HUB_VERSION=2.5.0
HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
GIT_REPO_URL=https://github.com/apache/beam.git
USER_REMOTE_URL=
USER_REMOTE_NAME=remote_repo
BEAM_ROOT_DIR=beam
BRANCH_NAME=snapshot_build

echo "Please enter your repo URL forked from apache beam:"
read USER_REMOTE_URL

echo "=================Environment Variables==============="
echo "Your beam repo URL: ${USER_REMOTE_URL}"
echo "Beam repo will be cloned into: ~/${LOCAL_BEAM_DIR}/"
echo "New PR will be created on branch: ${BRANCH_NAME}"

cd ~

echo "=================Checking hub========================"
if [[ -z `which hub` ]]; then
  echo "There is no hub installed on your machine."
  echo "Would you like to install hub with root permission? [y|N]"
  read confirmation
  if [[ $confirmation != "y"  ]]; then
    echo "Refused to install hub. Cannot proceed into next setp."
    exit
  fi
  echo "=================Installing hub======================="
  wget https://github.com/github/hub/releases/download/v${HUB_VERSION}/${HUB_ARTIFACTS_NAME}.tgz
  tar zvxvf ${HUB_ARTIFACTS_NAME}.tgz
  sudo ./${HUB_ARTIFACTS_NAME}/install
  echo "eval "$(hub alias -s)"" >> ~/.bashrc
  rm -rf ${HUB_ARTIFACTS_NAME}*
fi
hub version

echo "===============Starting creating empty PR==============="
cd ~
if [[ -d ${LOCAL_BEAM_DIR} ]]; then
  rm -rf ${LOCAL_BEAM_DIR}
fi
mkdir ${LOCAL_BEAM_DIR}
cd ${LOCAL_BEAM_DIR}
git clone ${GIT_REPO_URL}
cd ${BEAM_ROOT_DIR}
git remote add ${USER_REMOTE_NAME} ${USER_REMOTE_URL}
git checkout -b ${BRANCH_NAME}
touch empty_file.txt
git add -A
git commit -m "Add empty file in order to create PR"
git push -f ${USER_REMOTE_NAME}

cd ~/${LOCAL_BEAM_DIR}/${BEAM_ROOT_DIR}
hub pull-request -F- <<<"[DO NOT MERGE]Start snapshot build for release process


Run Gradle Publish"

echo "NOTE: If there is no jenkins job started, please comment generated PR with: Run Gradle Publish"

echo "===========================Cleaning up==========================="
cd ~
rm -rf ${LOCAL_BEAM_DIR}
echo "Things remained you need to do manually after build successful:"
echo "1. Close this generated PR in github website."
echo "2. Delete your remote branch ${BRANCH_NAME} form your beam repo in github website."
