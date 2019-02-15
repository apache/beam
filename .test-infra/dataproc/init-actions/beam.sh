#!/usr/bin/env bash

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
#    Pulls SDK Harness' images as specified in init action's metadata (see below).
#
#    NOTE: In order to be able to pull particular images, they must be present
#    in the repository in the following locations (by convention):
#
#    <beam image repository>/python
#    <beam image repository>/java
#    <beam image repository>/go
#
set -euxo pipefail

readonly BEAM_IMAGE_ENABLE_PULL_DEFAULT=false
readonly BEAM_PYTHON_IMAGE_ENABLE_PULL_METADATA_KEY="beam-python-image-enable-pull"
readonly BEAM_GO_IMAGE_ENABLE_PULL_METADATA_KEY="beam-go-image-enable-pull"
readonly BEAM_JAVA_IMAGE_ENABLE_PULL_METADATA_KEY="beam-java-image-enable-pull"
readonly BEAM_IMAGE_VERSION_METADATA_KEY="beam-image-version"
readonly BEAM_IMAGE_VERSION_DEFAULT="master"
readonly BEAM_IMAGE_REPOSITORY_KEY="beam-image-repository"
readonly BEAM_IMAGE_REPOSITORY_DEFAULT="apache.bintray.io/beam"

function pull_images() {
  local beam_image_version="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGE_VERSION_METADATA_KEY}" \
    || echo "${BEAM_IMAGE_VERSION_DEFAULT}")"
  local image_repo="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGE_REPOSITORY_KEY}" \
    || echo "${BEAM_IMAGE_REPOSITORY_DEFAULT}")"

  local pull_python_image="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_PYTHON_IMAGE_ENABLE_PULL_METADATA_KEY}" \
    || echo "${BEAM_IMAGE_ENABLE_PULL_DEFAULT}")"

  local pull_java_image="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_JAVA_IMAGE_ENABLE_PULL_METADATA_KEY}" \
    || echo "${BEAM_IMAGE_ENABLE_PULL_DEFAULT}")"

  local pull_go_image="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_GO_IMAGE_ENABLE_PULL_METADATA_KEY}" \
    || echo "${BEAM_IMAGE_ENABLE_PULL_DEFAULT}")"

  # Pull beam images with `sudo -i` since if pulling from GCR, yarn will be
  # configured with GCR authorization
  if ${pull_python_image} ; then
    sudo -u yarn -i docker pull "${image_repo}/python:${beam_image_version}"
  fi

  if ${pull_java_image} ; then
    sudo -u yarn -i docker pull "${image_repo}/java:${beam_image_version}"
  fi
  
  if ${pull_go_image} ; then
    sudo -u yarn -i docker pull "${image_repo}/go:${beam_image_version}"
  fi
}

function main() {
  pull_images
}

main "$@"