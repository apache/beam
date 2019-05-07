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

# This script will run pre-installations and run release build.

set -e

GIT_REPO_URL=https://github.com/apache/beam.git
LOCAL_CLONE_DIR=release_build
BEAM_ROOT_DIR=beam

echo "Which branch you want to verify release build: "
read branch

echo "=====================Environment Variables====================="
echo "working branch: ${branch}"
echo "local repo dir: ~/${LOCAL_CLONE_DIR}/${BEAM_ROOT_DIR}"

echo "====================Checking Requirement======================="

echo "====================Checking pip==============================="
if [[ -z `which pip` ]]; then
  echo "There is no pip installed on your machine."
  echo "Would you like to install pip with root permission? [y|N]"
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Refused to install pip on your machine. Exit."
    exit
  else
    echo "==================Installing pip========================="
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    sudo python get-pip.py
    rm get-pip.py
  fi
else
  pip --version
fi

echo "====================Checking virtualenv========================"
if [[ -z `which virtualenv` ]]; then
  echo "There is no virtualenv installed on your machine."
  echo "Would you like to install virtualenv with root perrission? [y|N]"
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Refused to install virtualenv on your machine. Exit."
    exit
  else
    echo "==================Installing virtualenv==================="
    sudo `which pip` install --upgrade virtualenv
  fi
else
  virtualenv --version
fi

echo "=====================Checking cython==========================="
if [[ -z `which cython` ]]; then
  echo "There is no cython installed on your machine."
  echo "Would you like to install cython with root permission? [y|N]"
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Refused to install cython on your machine. Exit."
    exit
  else
    echo "==================Installing cython======================="
    sudo `which pip` install cython
    sudo apt-get install gcc
    sudo apt-get install python-dev
  fi
else
  cython --version
fi

echo "==================Checking /usr/bin/time========================"
if [[ `which time` != "/usr/bin/time" ]]; then
  echo "There is no usr/bin/time installed on your machine."
  echo "Would you like to install time on your machine with root permission? [y|N]"
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Refused to install time on your machine. Exit."
    exit
  else
    echo "==================Installing time========================="
    sudo apt-get install time
    alias time='/usr/bin/time'
  fi
else
  which time
fi

echo "=================Checking hub========================"
HUB_VERSION=2.5.0
HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
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

cd ~
echo "======================Starting Clone Repo======================"
if [[ -d ${LOCAL_CLONE_DIR} ]]; then
  rm -rf ${LOCAL_CLONE_DIR}
fi
mkdir ${LOCAL_CLONE_DIR}
cd  ${LOCAL_CLONE_DIR}
git clone ${GIT_REPO_URL}
cd ${BEAM_ROOT_DIR}
git checkout ${branch}
echo "==============================================================="

echo "======================Starting Release Build==================="
git clean -fdx
./gradlew clean
# If build fails, we want to catch as much errors as possible once.
./gradlew build -PisRelease --scan --stacktrace --no-parallel --continue
echo "==============================================================="

echo "[Current Task] Run All PostCommit Tests against Release Branch"
echo "This task will create a PR against apache/beam."
echo "After PR created, you need to comment phrases listed in description in the created PR:"

echo "[Confirmation Required] Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "[Input Required] Please enter your github repo URL forked from apache/beam:"
  read USER_REMOTE_URL
  echo "[Input Required] Please enter your github username:"
  read GITHUB_USERNAME
  echo "[Input Required] Please enter your github token:"
  read GITHUB_TOKEN
  export GITHUB_TOKEN=${GITHUB_TOKEN}
  WORKING_BRANCH=postcommit_validation_pr
  git checkout -b ${WORKING_BRANCH}
  touch empty_file.txt
  git add empty_file.txt
  git commit -m "Add empty file in order to create PR"
  git push -f ${USER_REMOTE_URL}
  hub pull-request -o -b apache:${branch} -h ${GITHUB_USERNAME}:${WORKING_BRANCH} -F- <<<"[DO NOT MERGE] Run all PostCommit and PreCommit Tests against Release Branch

  Please comment as instructions below, one phrase per comment please:
  Run Go PostCommit
  Run Java PostCommit
  Run Java PortabilityApi PostCommit
  Run Java Flink PortableValidatesRunner Batch
  Run Java Flink PortableValidatesRunner Streaming'
  Run Apex ValidatesRunner
  Run Dataflow ValidatesRunner
  Run Flink ValidatesRunner
  Run Gearpump ValidatesRunner
  Run Dataflow PortabilityApi ValidatesRunner
  Run Samza ValidatesRunner
  Run Spark ValidatesRunner
  Run Python Dataflow ValidatesContainer
  Run Python Dataflow ValidatesRunner
  Run Python Flink ValidatesRunner
  Run Python PostCommit
  Run SQL PostCommit
  Run Go PreCommit
  Run Java PreCommit
  Run Java_Examples_Dataflow PreCommit
  Run JavaPortabilityApi PreCommit
  Run Portable_Python PreCommit
  Run Python PreCommit"

  echo "[NOTE]: Please make sure all test targets have been invoked."
  echo "Please check the test results. If there is any failure, follow the policy in release guide."
fi

echo "Do you want to clean local clone repo? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  cd ~
  rm -rf ${LOCAL_CLONE_DIR}
  echo "Clean up local repo."
fi
