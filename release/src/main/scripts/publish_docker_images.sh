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

DOCKER_IMAGE_DEFAULT_REPO_ROOT=apache
DOCKER_IMAGE_DEFAULT_REPO_PREFIX=beam_

PYTHON_VER=("python2.7" "python3.5" "python3.6" "python3.7" "python3.8")
FLINK_VER=("1.8" "1.9" "1.10")

echo "Publish SDK docker images to Docker Hub."

echo "================Setting Up Environment Variables==========="
echo "Which release version are you working on: "
read RELEASE

echo "Which release candidate will be the source of final docker images? (ex: 1)"
read RC_NUM
RC_VERSION="rc${RC_NUM}"

echo "================Confirming Release and RC version==========="
echo "We are using ${RC_VERSION} to push docker images for ${RELEASE}."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then

  echo '-------------------Tagging and Pushing Python images-----------------'
  for ver in "${PYTHON_VER[@]}"; do
    # Pull varified RC from dockerhub.
    docker pull ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_${RC_VERSION}

    # Tag with ${RELEASE} and push to dockerhub.
    docker tag ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_${RC_VERSION} ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}
    docker push ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}

    # Tag with latest and push to dockerhub.
    docker tag ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_${RC_VERSION} ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:latest
    docker push ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:latest

    # Cleanup images from local
    docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_${RC_VERSION}
    docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}
    docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:latest
  done

  echo '-------------------Tagging and Pushing Java images-----------------'
  # Pull varified RC from dockerhub.
  docker pull ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}_${RC_VERSION}

  # Tag with ${RELEASE} and push to dockerhub.
  docker tag ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}_${RC_VERSION} ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}
  docker push ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}

  # Tag with latest and push to dockerhub.
  docker tag ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}_${RC_VERSION} ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:latest
  docker push ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:latest

  # Cleanup images from local
  docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}_${RC_VERSION}
  docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}
  docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:latest

  echo '-------------Tagging and Pushing Flink job server images-------------'
  echo "Publishing images for the following Flink versions:" "${FLINK_VER[@]}"
  echo "Make sure the versions are correct, then press any key to proceed."
  read
  for ver in "${FLINK_VER[@]}"; do
    FLINK_IMAGE_NAME=${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}flink${ver}_job_server

    # Pull verified RC from dockerhub.
    docker pull "${FLINK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}"

    # Tag with ${RELEASE} and push to dockerhub.
    docker tag "${FLINK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}" "${FLINK_IMAGE_NAME}:${RELEASE}"
    docker push "${FLINK_IMAGE_NAME}:${RELEASE}"

    # Tag with latest and push to dockerhub.
    docker tag "${FLINK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}" "${FLINK_IMAGE_NAME}:latest"
    docker push "${FLINK_IMAGE_NAME}:latest"

    # Cleanup images from local
    docker rmi -f "${FLINK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}"
    docker rmi -f "${FLINK_IMAGE_NAME}:${RELEASE}"
    docker rmi -f "${FLINK_IMAGE_NAME}:latest"
  done

  echo '-------------Tagging and Pushing Spark job server image-------------'
  SPARK_IMAGE_NAME=${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}spark_job_server

  # Pull verified RC from dockerhub.
  docker pull "${SPARK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}"

  # Tag with ${RELEASE} and push to dockerhub.
  docker tag "${SPARK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}" "${SPARK_IMAGE_NAME}:${RELEASE}"
  docker push "${SPARK_IMAGE_NAME}:${RELEASE}"

  # Tag with latest and push to dockerhub.
  docker tag "${SPARK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}" "${SPARK_IMAGE_NAME}:latest"
  docker push "${SPARK_IMAGE_NAME}:latest"

  # Cleanup images from local
  docker rmi -f "${SPARK_IMAGE_NAME}:${RELEASE}_${RC_VERSION}"
  docker rmi -f "${SPARK_IMAGE_NAME}:${RELEASE}"
  docker rmi -f "${SPARK_IMAGE_NAME}:latest"
fi
