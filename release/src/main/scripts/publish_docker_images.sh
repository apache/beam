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

echo "================Pull RC Containers from DockerHub==========="
IMAGES=$(docker search ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX} --format "{{.Name}}" --limit 100)
KNOWN_IMAGES=()
echo "We are using ${RC_VERSION} to push docker images for ${RELEASE}."
while read IMAGE; do
  # Try pull verified RC from dockerhub.
  if docker pull "${IMAGE}:${RELEASE}${RC_VERSION}" 2>/dev/null ; then
    KNOWN_IMAGES+=( $IMAGE )
  fi
done < <(echo "${IMAGES}")

echo "================Confirming Release and RC version==========="
echo "Publishing the following images:"
# Sort by name for easy examination
IFS=$'\n' KNOWN_IMAGES=($(sort <<<"${KNOWN_IMAGES[*]}"))
unset IFS
printf "%s\n" ${KNOWN_IMAGES[@]}
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  for IMAGE in "${KNOWN_IMAGES[@]}"; do
    # Perform a carbon copy of ${RC_VERSION} to dockerhub with a new tag as ${RELEASE}.
    docker buildx imagetools create --tag "${IMAGE}:${RELEASE}" "${IMAGE}:${RELEASE}${RC_VERSION}"

    # Perform a carbon copy of ${RC_VERSION} to dockerhub with a new tag as latest.
    docker buildx imagetools create --tag "${IMAGE}:latest" "${IMAGE}:${RELEASE}"
  done

fi
