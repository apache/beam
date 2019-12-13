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

# This script will generate and publish docker images for each language version to Docker Hub:
# 1. Generate images tagged with :{RELEASE}
# 2. Publish images tagged with :{RELEASE}
# 3. Tag images with :latest tag and publish.
# 4. Clean up images.

set -e

source release/src/main/scripts/build_release_candidate.sh

echo "Publish SDK docker images to Docker Hub."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Publishing SDK docker images on docker hub========="
  cd ~
  if [[ -d ${LOCAL_PYTHON_STAGING_DIR} ]]; then
    rm -rf ${LOCAL_PYTHON_STAGING_DIR}
  fi
  mkdir -p ${LOCAL_PYTHON_STAGING_DIR}
  cd ${LOCAL_PYTHON_STAGING_DIR}

  echo '-------------------Cloning Beam Release Branch-----------------'
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}

  echo '-------------------Generating and Pushing Python images-----------------'
  ./gradlew :sdks:python:container:buildAll -Pdocker-tag=${RELEASE}
  for ver in "${PYTHON_VER[@]}"; do
     docker push apachebeam/${ver}_sdk:${RELEASE}
     docker tag apachebeam/${ver}_sdk:${RELEASE} apachebeam/${ver}_sdk:latest
     docker push apachebeam/${ver}_sdk:latest
  done

  echo '-------------------Generating and Pushing Java images-----------------'
  ./gradlew :sdks:java:container:dockerPush -Pdocker-tag=${RELEASE}
  docker tag apachebeam/java_sdk:${RELEASE} apachebeam/java_sdk:latest
  docker push apachebeam/java_sdk:latest

  echo '-------------------Generating and Pushing Go images-----------------'
  ./gradlew :sdks:go:container:dockerPush -Pdocker-tag=${RELEASE}
  docker tag apachebeam/go_sdk:${RELEASE} apachebeam/go_sdk:latest
  docker push apachebeam/go_sdk:latest

  echo '-------------Generating and Pushing Flink job server images-------------'
  echo "Building containers for the following Flink versions:" "${FLINK_VER[@]}"
  for ver in "${FLINK_VER[@]}"; do
     ./gradlew ":runners:flink:${ver}:job-server-container:docker" -Pdocker-tag="${RELEASE}"
     FLINK_IMAGE_NAME=apachebeam/flink${ver}_job_server
     docker push "${FLINK_IMAGE_NAME}:${RELEASE}"
     docker tag "${FLINK_IMAGE_NAME}:${RELEASE}" "${FLINK_IMAGE_NAME}:latest"
     docker push "${FLINK_IMAGE_NAME}:latest"
  done

  rm -rf ~/${PYTHON_ARTIFACTS_DIR}

  echo "-------------------Clean up SDK docker images at local-------------------"
  for ver in "${PYTHON_VER[@]}"; do
    docker rmi -f apachebeam/${ver}_sdk:${RELEASE}
    docker rmi -f apachebeam/${ver}_sdk:latest
  done

  docker rmi -f apachebeam/java_sdk:${RELEASE}
  docker rmi -f apachebeam/java_sdk:latest

  docker rmi -f apachebeam/go_sdk:${RELEASE}
  docker rmi -f apachebeam/go_sdk:latest

  for ver in "${FLINK_VER[@]}"; do
    FLINK_IMAGE_NAME=apachebeam/flink${ver}_job_server
    docker rmi -f "${FLINK_IMAGE_NAME}:${RELEASE}"
    docker rmi -f "${FLINK_IMAGE_NAME}:latest"
  done
fi
