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
gpg --output ~/doc.sig --sign ~/.bashrc
# If build fails, we want to catch as much errors as possible once.
./gradlew build -PisRelease --scan --stacktrace --no-parallel --continue
echo "==============================================================="

echo "Do you want to clean local clone repo? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  cd ~
  rm -rf ${LOCAL_CLONE_DIR}
  echo "Clean up local repo."
fi
