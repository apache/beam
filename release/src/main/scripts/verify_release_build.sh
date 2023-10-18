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

# This script helps to verify full life cycle of Gradle build and all
# PostCommit tests against release branch on Jenkins.
#
# It reads configurations from script.config, setup environment and finally
# create a test PR to run Jenkins jobs.
#
# NOTE:
#   1. Please create a personal access token from your Github account first.
#      Instructions: https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line
#   2. Please set RELEASE_BUILD_CONFIGS in script.config before running this
#      script.
#   3. Please manually comment trigger phrases to the created PR to start
#      Gradle release build and all PostCommit jobs, or run mass_comment.py
#      to do so. Phrases are listed in COMMENTS_TO_ADD in mass_comment.py.


. script.config

set -e

BEAM_REPO_URL=https://github.com/apache/beam.git
RELEASE_BRANCH=release-${RELEASE_VER}
WORKING_BRANCH=postcommit_validation_pr
SCRIPT=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT)

function clean_up(){
  echo ""
  echo "==================== Final Cleanup ===================="
  rm -rf ${LOCAL_BEAM_DIR}
  echo "* Deleted workspace ${LOCAL_BEAM_DIR}"
}
trap clean_up EXIT


echo ""
echo "==================== 1 Checking Environment Variables ================="
echo "* PLEASE update RELEASE_BUILD_CONFIGS in file script.config first *"
echo ""
echo "Verify release build against branch: ${RELEASE_BRANCH}."
echo "Use workspace: ${LOCAL_BEAM_DIR}"
echo ""
echo "All environment and workflow configurations from RELEASE_BUILD_CONFIGS:"
for i in "${RELEASE_BUILD_CONFIGS[@]}"; do
  echo "$i = ${!i}"
done
echo "[Confirmation Required] Are they all provided and correctly set? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right configurations."
  exit
fi


echo ""
echo "==================== 2 Checking Requirements ======================="

echo "====================== 2.1 Checking git ========================"
if [[ -z ${GITHUB_TOKEN} ]]; then
  echo "Error: A Github personal access token is required to perform git push "
  echo "under a newly cloned directory. Please manually create one from Github "
  echo "website with guide:"
  echo "https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line"
  echo "Note: This token can be reused in other release scripts."
  exit
else
  echo "====================== Cloning repo ======================"
  git clone ${BEAM_REPO_URL} ${LOCAL_BEAM_DIR}
  cd ${LOCAL_BEAM_DIR}
  # Set upstream repo url with access token included.
  USER_REPO_URL=https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/${GITHUB_USERNAME}/beam.git
  git remote add ${GITHUB_USERNAME} ${USER_REPO_URL}
  # For hub access Github API.
  export GITHUB_TOKEN=${GITHUB_TOKEN}
  # For local git repo only. Required if global configs are not set.
  git config user.name "${GITHUB_USERNAME}"
  git config user.email "${GITHUB_USERNAME}@gmail.com"
fi

echo "====================== 2.2 Checking hub ========================"
HUB_VERSION=2.12.0

if [[ "$(uname)" ==  "Linux" ]]; then
  ARCITECTURE=linux-amd64
elif [[ "$(uname)" ==  "Darwin" ]]; then
  ARCITECTURE=darwin-amd64
else
  echo "Error: unsupported architecture for Hub"
  exit
fi
HUB_ARTIFACTS_NAME=hub-${ARCITECTURE}-${HUB_VERSION}

if [[ -z `which hub` ]]; then
  echo "There is no hub installed on your machine."
  if [[ "${INSTALL_HUB}" = true  ]]; then
    echo "====================== Installing hub ======================="
    wget https://github.com/github/hub/releases/download/v${HUB_VERSION}/${HUB_ARTIFACTS_NAME}.tgz
    tar zvxvf ${HUB_ARTIFACTS_NAME}.tgz
    sudo ./${HUB_ARTIFACTS_NAME}/install
    echo "eval "$(hub alias -s)"" >> ~/.bashrc
    rm -rf ${HUB_ARTIFACTS_NAME}*
  else
    echo "Refused to install hub. Cannot proceed into next setp."; exit
  fi
fi
hub version


echo ""
echo "==================== 3 Run PostCommit Tests on Jenkins ==================="
echo "[Current Task] Run all PostCommit Tests against Release Branch on Jenkins."
echo "This task will create a PR against apache/beam."
echo "After PR created, you need to comment phrases listed in description in the created PR:"

if [[ ! -z `which hub` ]]; then
  git checkout -b ${WORKING_BRANCH} origin/${RELEASE_BRANCH} --quiet
  # The version change is needed for Dataflow python batch tests.
  # Without changing to dev version, the dataflow pipeline will fail because of non-existed worker containers.
  # Note that dataflow worker containers should be built after RC has been built.
  bash "$SCRIPT_DIR"/set_version.sh "$RELEASE_VER" --git-add
  # add a file that will trigger all relevant GHA workflows. Need to be .json extension to be excluded from RAT check
  echo "{}" > release/trigger_all_tests.json
  git add release/trigger_all_tests.json
  git commit -m "Changed version.py and gradle.properties to python dev version to create a test PR" --quiet
  git push -f ${GITHUB_USERNAME} ${WORKING_BRANCH} --quiet

  hub pull-request -b apache:${RELEASE_BRANCH} -h ${GITHUB_USERNAME}:${WORKING_BRANCH} -F- <<<"[DO NOT MERGE] Run all PostCommit and PreCommit Tests against Release Branch

  You can run many tests automatically using release/src/main/scripts/mass_comment.py."

  echo ""
  echo "[NOTE]: Please make sure all test targets (GHA and Jenkins) have been invoked."
  echo "Please check the test results. If there is any failure, follow the policy in release guide."
fi
