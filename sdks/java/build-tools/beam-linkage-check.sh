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

# This script compares linkage errors (checkJavaLinkage task in the root gradle project) between
# one branch and master branch.
# This is a temporary solution before Linkage Checker implements exclusion rules (BEAM-9206).

# Usage:
#  /bin/bash sdks/java/build-tools/beam-linkage-check.sh
#
#  By default, this checks the Maven artifacts listed in ARTIFACTS variable below.
#
#  Optionally, you can pass artifact ID to overwrite the ARTIFACTS, such as:
#    /bin/bash sdks/java/build-tools/beam-linkage-check.sh "beam-runners-google-cloud-dataflow-java"
#
#  Multiple artifact IDs (separated by a comma) will check linkage errors that would appear when
#  the two artifacts are used together. For example:
#    /bin/bash sdks/java/build-tools/beam-linkage-check.sh "beam-runners-google-cloud-dataflow-java,beam-sdks-java-io-hadoop-format"

set -o pipefail
set -e

# These default artifacts are common causes of linkage errors.
ARTIFACTS="beam-sdks-java-core
  beam-sdks-java-io-google-cloud-platform
  beam-runners-google-cloud-dataflow-java
  beam-sdks-java-io-hadoop-format"

if [ ! -z "$1" ]; then
  ARTIFACTS=$1
fi

BRANCH_NAME=$(git symbolic-ref --short HEAD)

if [ ! -d buildSrc ]; then
  echo "Please run this script in the Beam project root:"
  echo "  /bin/bash sdks/java/build-tools/beam-linkage-check.sh"
  exit 1
fi

if [ "$BRANCH_NAME" = "master" ]; then
  echo "Please run this script on a branch other than master"
  exit 1
fi

OUTPUT_DIR=build/linkagecheck
mkdir -p $OUTPUT_DIR

if [ ! -z "$(git diff)" ]; then
  echo "Uncommited change detected. Please commit changes and ensure 'git diff' is empty."
  exit 1
fi

ACCUMULATED_RESULT=0

function runLinkageCheck () {
  COMMIT=$1
  BRANCH=$2
  MODE=$3 # "baseline" or "validate"
  for ARTIFACT in $ARTIFACTS; do
    echo "`date`:" "Running linkage check (${MODE}) for ${ARTIFACT} in ${BRANCH}"

    BASELINE_FILE=${OUTPUT_DIR}/baseline-${ARTIFACT}.xml
    if [ "$MODE" = "baseline" ]; then
      BASELINE_OPTION="-PjavaLinkageWriteBaseline=${BASELINE_FILE}"
      echo "`date`:" "to create a baseline (existing errors before change) $BASELINE_FILE"
    elif [ "$MODE" = "validate" ]; then
      BASELINE_OPTION="-PjavaLinkageReadBaseline=${BASELINE_FILE}"
      echo "`date`:" "using baseline $BASELINE_FILE"
    else
      BASELINE_OPTION=""
      echo "`date`:" "Unexpected mode: ${MODE}. Not using baseline file."
    fi

    set +e
    ./gradlew -Ppublishing -PjavaLinkageArtifactIds=$ARTIFACT ${BASELINE_OPTION} :checkJavaLinkage
    RESULT=$?
    set -e
    if [ "$MODE" = "validate" ]; then
      echo "`date`:" "Done: ${RESULT}"
      ACCUMULATED_RESULT=$((ACCUMULATED_RESULT | RESULT))
    fi
  done
}


BRANCH_NAME=`git rev-parse --abbrev-ref HEAD`
BRANCH_COMMIT=`git rev-parse --short=8 HEAD`

git fetch
MASTER_COMMIT=`git rev-parse --short=8 origin/master`
git -c advice.detachedHead=false checkout $MASTER_COMMIT
runLinkageCheck $MASTER_COMMIT master baseline


# Restore original branch
git checkout $BRANCH_NAME
runLinkageCheck $BRANCH_COMMIT $BRANCH_NAME validate

if [ "${ACCUMULATED_RESULT}" = "0" ]; then
  echo "No new linkage errors"
else
  echo "There's new linkage errors. See above for details."
fi

# CI-friendly way to tell the result
exit $ACCUMULATED_RESULT
