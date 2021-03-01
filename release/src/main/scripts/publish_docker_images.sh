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

echo "Publish SDK docker images to Docker Hub."

echo "================Setting Up Environment Variables==========="
echo "Which release version are you working on: "
read RELEASE

echo "Which release candidate will be the source of final docker images? (ex: 1)"
read RC_NUM
RC_VERSION="rc${RC_NUM}"

echo "================Confirming Release and RC version==========="
echo "We are using ${RC_VERSION} to push docker images for ${RELEASE}."
echo "Publishing the following images:"
docker images --filter "reference=apache/beam_*:${RELEASE}_${RC_VERSION}" --format "{{.Repository}}" | while read IMAGE; do
  echo "${IMAGE}"
done
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  docker images --filter "reference=apache/beam_*:${RELEASE}_${RC_VERSION}" --format "{{.Repository}}" | while read IMAGE; do
    # Pull verified RC from dockerhub.
    docker pull "${IMAGE}:${RELEASE}_${RC_VERSION}"

    # Tag with ${RELEASE} and push to dockerhub.
    docker tag "${IMAGE}:${RELEASE}_${RC_VERSION}" "${IMAGE}:${RELEASE}"
    docker push "${IMAGE}:${RELEASE}"

    # Tag with latest and push to dockerhub.
    docker tag "${IMAGE}:${RELEASE}_${RC_VERSION}" "${IMAGE}:latest"
    docker push "${IMAGE}:latest"
  done

fi