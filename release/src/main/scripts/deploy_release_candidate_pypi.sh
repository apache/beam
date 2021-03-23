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

# This script will deploy a Release Candidate to pypi, includes:
# 1. Download python binary artifacts
# 2. Deploy Release Candidate to pypi

set -e

function clean_up(){
  echo "Do you want to clean local clone repo ${LOCAL_CLONE_DIR}? [y|N]"
  read confirmation
  if [[ $confirmation = "y" ]]; then
    cd ~
    rm -rf ${LOCAL_CLONE_DIR}
    echo "Cleaned up local repo."
  fi
}

GIT_REPO_BASE_URL=apache/beam
PYTHON_ARTIFACTS_DIR=python
BEAM_ROOT_DIR=beam

echo "================Setting Up Environment Variables==========="
if [[ -z "${RELEASE}" ]]
then
  echo "Which release version are you working on: "
  read RELEASE
fi
RELEASE_BRANCH="release-${RELEASE}"
if [[ -z "${RC_NUM}" ]]
then
  echo "Which release candidate number(e.g. 1) are you going to create: "
  read RC_NUM
fi
if [[ -z "${USER_GITHUB_ID}" ]]
then
  echo "Please enter your github username(ID): "
  read USER_GITHUB_ID
fi
echo "${RELEASE_BRANCH}"
RELEASE_COMMIT=$(git rev-parse --verify "${RELEASE_BRANCH}")
LOCAL_CLONE_DIR="beam_release_${RELEASE}rc${RC_NUM}"

echo "================Checking Environment Variables=============="
echo "working on release version: ${RELEASE}"
echo "working on release branch: ${RELEASE_BRANCH}"
echo "will create release candidate: RC${RC_NUM}"
echo "Please review all environment variables and confirm: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "=====================Clear folder=============================="
cd ~
if [[ -d ${LOCAL_CLONE_DIR} ]]; then
  echo "Deleting existing local clone repo ${LOCAL_CLONE_DIR}."
  rm -rf "${LOCAL_CLONE_DIR}"
fi
mkdir "${LOCAL_CLONE_DIR}"
LOCAL_CLONE_DIR_ROOT=$(pwd)/${LOCAL_CLONE_DIR}

echo "================Download python artifacts======================"
cd -
python ./download_github_actions_artifacts.py \
  --github-user "${USER_GITHUB_ID}" \
  --repo-url "${GIT_REPO_BASE_URL}" \
  --release-branch "${RELEASE_BRANCH}" \
  --release-commit "${RELEASE_COMMIT}" \
  --artifacts_dir "${LOCAL_CLONE_DIR_ROOT}" \
  --is_rc_version

cd ${LOCAL_CLONE_DIR_ROOT}

echo "------Checking Hash Value for apache-beam-${RELEASE}rc${RC_NUM}.zip-----"
sha512sum -c "apache-beam-${RELEASE}rc${RC_NUM}.zip.sha512"

for artifact in *.whl; do
  echo "----------Checking Hash Value for ${artifact} wheel-----------"
  sha512sum -c "${artifact}.sha512"
done

echo "===================Removing sha512 files======================="
rm $(ls | grep -i ".*.sha512$")

echo "====================Upload rc to pypi========================"
virtualenv deploy_pypi_env
source ./deploy_pypi_env/bin/activate
pip install twine

mkdir dist && mv $(ls | grep apache) dist && cd dist
echo "Will upload the following files to PyPI:"
ls
echo "Are the files listed correct? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Exiting without deploying artifacts to PyPI."
  clean_up
  exit
fi
twine upload *
clean_up