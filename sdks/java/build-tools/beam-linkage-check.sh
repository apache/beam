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
# one branch and another.

# Usage:
#  /bin/bash sdks/java/build-tools/beam-linkage-check.sh origin/master <your branch>
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
DEFAULT_ARTIFACT_LISTS=" \
  beam-sdks-java-core \
  beam-sdks-java-io-google-cloud-platform \
  beam-runners-google-cloud-dataflow-java \
  beam-sdks-java-io-hadoop-format \
"

BASELINE_REF=$1
PROPOSED_REF=$2
ARTIFACT_LISTS=$3

if [ -z "$ARTIFACT_LISTS" ]; then
  ARTIFACT_LISTS=$DEFAULT_ARTIFACT_LISTS
fi

if [ -z "$BASELINE_REF" ] || [ -z "$PROPOSED_REF" ] || [ -z "$ARTIFACT_LISTS" ] ; then
  echo "Usage: $0 <baseline ref> <proposed ref> [artifact lists]"
  exit 1
fi

if [ ! -d buildSrc ]; then
  echo "Directory 'buildSrc' not found. Please run this script from the root directory of a clone of the Beam git repo."
fi

if [ "$BASELINE_REF" = "$PROPOSED_REF" ]; then
  echo "Baseline and proposed refs are identical; cannot compare their linkage errors!"
  exit 1
fi

if [ ! -z "$(git diff)" ]; then
  echo "Uncommited change detected. Please commit changes and ensure 'git diff' is empty."
  exit 1
fi

STARTING_REF=$(git rev-parse --abbrev-ref HEAD)
function cleanup() {
  git checkout $STARTING_REF
}
trap cleanup EXIT

echo "Comparing linkage of artifact lists $ARTIFACT_LISTS using baseline $BASELINE_REF and proposal $PROPOSED_REF"

OUTPUT_DIR=build/linkagecheck
mkdir -p $OUTPUT_DIR

ACCUMULATED_RESULT=0

function runLinkageCheck () {
  MODE=$1 # "baseline" or "validate"

  for ARTIFACT_LIST in $ARTIFACT_LISTS; do
    echo "`date`:" "Running linkage check (${MODE}) for ${ARTIFACT_LIST}"

    BASELINE_FILE=${OUTPUT_DIR}/baseline-${ARTIFACT_LIST}.xml
    if [ "$MODE" = "baseline" ]; then
      BASELINE_OPTION="-PjavaLinkageWriteBaseline=${BASELINE_FILE}"
      echo "`date`:" "to create a baseline (existing errors before change) $BASELINE_FILE"
    elif [ "$MODE" = "validate" ]; then
      BASELINE_OPTION="-PjavaLinkageReadBaseline=${BASELINE_FILE}"
      echo "`date`:" "using baseline $BASELINE_FILE"
      if [ ! -r "${BASELINE_FILE}" ]; then
        # If baseline generation failed in previous baseline step, no need to build the project.
        echo "`date`:" "Error: Baseline file not found"
        ACCUMULATED_RESULT=1
        continue
      fi
    else
      BASELINE_OPTION=""
      echo "`date`:" "Unexpected mode: ${MODE}. Not using baseline file."
    fi

    set +e
    set -x
    ./gradlew -Ppublishing -PskipCheckerFramework -PjavaLinkageArtifactIds=$ARTIFACT_LIST ${BASELINE_OPTION} :checkJavaLinkage
    RESULT=$?
    set -e
    set +x
    if [ "$MODE" = "baseline" ] && [ ! -r "${BASELINE_FILE}" ]; then
      echo "`date`:" "Failed to generate the baseline file. Check the build error above."
      ACCUMULATED_RESULT=1
    elif [ "$MODE" = "validate" ]; then
      echo "`date`:" "Done: ${RESULT}"
      ACCUMULATED_RESULT=$((ACCUMULATED_RESULT | RESULT))
    fi
  done
}

echo "Establishing baseline linkage for $(git rev-parse --abbrev-ref $BASELINE_REF)"
git -c advice.detachedHead=false checkout $BASELINE_REF
runLinkageCheck baseline

echo "Checking linkage for $(git rev-parse --abbrev-ref $PROPOSED_REF)"
git -c advice.detachedHEad=false checkout $PROPOSED_REF
runLinkageCheck validate

if [ "${ACCUMULATED_RESULT}" = "0" ]; then
  echo "No new linkage errors"
else
  echo "There's new linkage errors. See above for details."
fi

# CI-friendly way to tell the result
exit $ACCUMULATED_RESULT
