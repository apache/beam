#!/usr/bin/env bash
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
#    Delete stale python prebuilt SDK container images older than one day.
#
set -euo pipefail

REPOSITORIES=(prebuilt_beam_sdk/beam_python_prebuilt_sdk beam-sdk beam_portability)

echo $REPOSITORIES

for repository in ${REPOSITORIES[@]}; do
  echo IMAGES FOR REPO ${repository}
  IMAGE_NAMES+=$(gcloud container images list --repository=gcr.io/apache-beam-testing/${repository} --format="get(name)")
  IMAGE_NAMES+=" "
done

echo $IMAGE_NAMES

for image_name in ${IMAGE_NAMES[@]}; do
  echo IMAGES FOR image ${image_name}
  echo "Command" gcloud container images list-tags \
  ${image_name} \
  --sort-by=TIMESTAMP  --filter="NOT tags:latest AND timestamp.datetime < $(date --iso-8601=s -d '5 days ago')" \
  --format="get(digest)"
  STALE_IMAGES_CURRENT=$(gcloud container images list-tags \
   ${image_name} \
    --sort-by=TIMESTAMP  --filter="NOT tags:latest AND timestamp.datetime < $(date --iso-8601=s -d '5 days ago')" \
    --format="get(digest)")
  STALE_IMAGES+=$STALE_IMAGES_CURRENT
  for current in ${STALE_IMAGES_CURRENT[@]}; do
    echo "Deleting image. Command: gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q"
    gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q
  done
done


if [[ ${STALE_IMAGES} ]]; then
  echo "Deleted multiple images"
else
  echo "No stale prebuilt container images found."
fi
