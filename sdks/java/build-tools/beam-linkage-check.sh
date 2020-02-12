#!/bin/bash

# Script to compare linkage errors (checkJavaLinkage task in the root gradle project) between PR's
# branch and master branch.
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

function runLinkageCheck () {
  COMMIT=$1
  local BRANCH_NAME=$(git symbolic-ref --short HEAD)
  # An empty invocation so that the subsequent checkJavaLinkage does not
  # contain garbage
  echo "`date`:" "Installing artifacts of ${BRANCH_NAME}(${COMMIT}) to Maven local repository."
  ./gradlew -Ppublishing -PjavaLinkageArtifactIds=beam-sdks-java-core :checkJavaLinkage > /dev/null 2>&1
  for ARTIFACT in $ARTIFACTS; do
    echo "`date`:" "Running linkage check for ${ARTIFACT} in ${BRANCH_NAME}"
    # Removing time taken to have clean diff
    ./gradlew -Ppublishing -PjavaLinkageArtifactIds=$ARTIFACT :checkJavaLinkage |grep -v 'BUILD SUCCESSFUL in' | grep -v 'dependency paths' > ${OUTPUT_DIR}/${COMMIT}-${ARTIFACT}
    echo "`date`:" "Done: ${OUTPUT_DIR}/${COMMIT}-${ARTIFACT}"
  done
}

BRANCH_COMMIT=`git rev-parse --short=8 HEAD`
runLinkageCheck $BRANCH_COMMIT

git checkout master
git pull
MASTER_COMMIT=`git rev-parse --short=8 HEAD`
runLinkageCheck $MASTER_COMMIT

# Restore original branch
git checkout $BRANCH_NAME

# Diff command can return non-zero status
set +e

FOUND_DIFFERENCE=false
for ARTIFACT in $ARTIFACTS; do
  echo; echo
  echo "Linkage Check difference on $ARTIFACT between master(${MASTER_COMMIT}) and ${BRANCH_NAME}(${BRANCH_COMMIT}):"
  DIFF=$(diff ${OUTPUT_DIR}/${MASTER_COMMIT}-${ARTIFACT} ${OUTPUT_DIR}/${BRANCH_COMMIT}-${ARTIFACT})
  if [ -z "$DIFF" ]; then
   echo "(no difference)"
  else
    FOUND_DIFFERENCE=true
    echo "Lines starting with '<' mean the branch remedies the errors (good)"
    echo "Lines starting with '>' mean the branch introduces new errors (bad)"
    diff ${OUTPUT_DIR}/${MASTER_COMMIT}-${ARTIFACT} ${OUTPUT_DIR}/${BRANCH_COMMIT}-${ARTIFACT}
  fi
done

if [ "$FOUND_DIFFERENCE" == true ]; then
  exit 1
fi