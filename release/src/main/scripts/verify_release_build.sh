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
echo "==============================================================="

echo "====================Starting Pre-installation=================="
cd ~
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
sudo python get-pip.py
rm get-pip.py

sudo `which pip` install --upgrade virtualenv

sudo `which pip` install cython
sudo apt-get install gcc
sudo apt-get install python-dev

sudo apt-get install time
alias time='/usr/bin/time'
echo "==============================================================="

echo "======================Starting Clone Repo======================"
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
./gradlew build -PisRelease --scan --stacktrace
echo "==============================================================="

echo "Do you want to clean local clone repo? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  cd ~
  rm -rf ${LOCAL_CLONE_DIR}
  echo "Clean up local repo."
fi
