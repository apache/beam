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
#    Pulls SDK Harness' and/or Job Server's images based on init action's metadata:
#
#    beam-sdk-harness-images-to-pull=gcr.io/<IMAGE_REPOSITORY>/<IMAGE_NAME>:<IMAGE_REVISION> gcr.io/<IMAGE_REPOSITORY>/<IMAGE_NAME>:<IMAGE_REVISION>
#    beam-job-server-image=gcr.io/<IMAGE_REPOSITORY>/<IMAGE_NAME>:<IMAGE_REVISION>
#
#    If you want to skip pulling job server / harness images simply don't pass urls for them.
#
set -euxo pipefail

readonly BEAM_HARNESS_IMAGES_TO_PULL_METADATA_KEY="beam-sdk-harness-images-to-pull"
readonly JOB_SERVER_IMAGE_METADATA_KEY="beam-job-server-image"

function pull_images() {

  local beam_images_to_pull="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_HARNESS_IMAGES_TO_PULL_METADATA_KEY}")"

  for image in $beam_images_to_pull
  do
    echo "Pulling image: ${image}"

    # Pull beam images with `sudo -i` since if pulling from GCR, yarn will be
    # configured with GCR authorization
    sudo -u yarn -i docker pull ${image}
  done
}

function pull_job_server() {
  local job_server_location="$(/usr/share/google/get_metadata_value \
    "attributes/${JOB_SERVER_IMAGE_METADATA_KEY}")"

  if [[ -n "$job_server_location" ]] ; then
    sudo -u yarn -i docker pull ${job_server_location}
  fi

}

function main() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  pull_images

  if [[ "${role}" == 'Master' ]] ; then
    pull_job_server || err "Unable to pull Beam Job Server image on Flink Master"
  fi
}

main "$@"