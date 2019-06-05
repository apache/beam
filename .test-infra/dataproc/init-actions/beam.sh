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
#    NOTE: In order to be able to pull particular images, their urls must be passed in metadata
#    in the form of list, like this:
#
#    beam-images-to-pull=gcr.io/<IMAGE_REPOSITORY>/<IMAGE_NAME>:<IMAGE_REVISION> gcr.io/<IMAGE_REPOSITORY>/<IMAGE_NAME>:<IMAGE_REVISION>
#
set -euxo pipefail

readonly BEAM_IMAGES_TO_PULL_METADATA_KEY="beam-images-to-pull"
readonly BEAM_IMAGES_TO_PULL_DEFAULT="apache.bintray.io/beam/python:master"


function pull_images() {

  local beam_images_to_pull="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGES_TO_PULL_METADATA_KEY}" \
    || echo "${BEAM_IMAGES_TO_PULL_DEFAULT}")"

  for image in $beam_images_to_pull
  do
    echo "Pulling image: ${image}"

    # Pull beam images with `sudo -i` since if pulling from GCR, yarn will be
    # configured with GCR authorization
    sudo -u yarn -i docker pull ${image}
  done
}

function main() {
  pull_images
}

main "$@"
