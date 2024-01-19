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

# Clean up private registry (us.gcr.io)
# Images more than 5 day old and not the latest (either has latest label or newest)

PUBLIC_REPOSITORIES=(beam-sdk beam_portability beamgrafana beammetricssyncjenkins beammetricssyncgithub)
PRIVATE_REPOSITORIES=(java-postcommit-it python-postcommit-it jenkins github-actions)
# set as the same as 6-week release period
DELETE_BEFORE_DAY=$(date --iso-8601=s -d '6 weeks ago')

REPOSITORIES=("${PUBLIC_REPOSITORIES[@]/#/gcr.io/apache-beam-testing/}" "${PRIVATE_REPOSITORIES[@]/#/us.gcr.io/apache-beam-testing/}")

echo $REPOSITORIES

# walk repos recursively
IMAGE_NAMES=""
while [ -n "$REPOSITORIES" ]; do
  PENDING_REPOSITORIES=""
  for repository in ${REPOSITORIES[@]}; do
    IMAGE_NAME=$(gcloud container images list --repository=${repository} --format="get(name)")
    if [ -n "$IMAGE_NAME" ]; then
      PENDING_REPOSITORIES+=$IMAGE_NAME
      PENDING_REPOSITORIES+=" "
    else
      echo IMAGES FOR REPO ${repository}
      IMAGE_NAMES+=$repository
      IMAGE_NAMES+=" "
    fi
  done
  REPOSITORIES=("${PENDING_REPOSITORIES[@]}")
done

for image_name in ${IMAGE_NAMES[@]}; do
  echo IMAGES FOR image ${image_name}
  FAILED_TO_DELETE=""
  # get the newest image without latest label
  LATEST_IN_TIME=$(gcloud container images list-tags \
     ${image_name} --sort-by="~TIMESTAMP"  --filter="NOT tags:latest " --format="get(digest)" --limit=1)
  if [ -n "$LATEST_IN_TIME" ]; then
    # list containers of the image name
    echo "Command" gcloud container images list-tags \
    ${image_name} \
    --sort-by=TIMESTAMP  --filter="NOT tags:latest AND timestamp.datetime < $DELETE_BEFORE_DAY" \
    --format="get(digest)"
    STALE_IMAGES_CURRENT=$(gcloud container images list-tags \
     ${image_name} \
      --sort-by=TIMESTAMP  --filter="NOT tags:latest AND timestamp.datetime < $DELETE_BEFORE_DAY" \
      --format="get(digest)")
    STALE_IMAGES+=$STALE_IMAGES_CURRENT
    for current in ${STALE_IMAGES_CURRENT[@]}; do
      # do not delete the one with latest label and the newest image without latest label
      # this make sure we leave at least one container under each image name, either labelled "latest" or not
      if [ "$LATEST_IN_TIME" != "$current" ]; then
        # Check to see if this image is built on top of earlier images. This is the case for multiarch images,
        # they will have a virtual size of 0 and a created date at the start of the epoch, but their manifests will
        # point to active images. These images should only be deleted when all of their dependencies can be safely
        # deleted.
        MANIFEST=$(docker manifest inspect ${image_name}@"${current}")
        SHOULD_DELETE=0
        DIGEST=$(echo $MANIFEST |  jq -r '.manifests[0].digest')
        if [ "$DIGEST" != "null" ]
        then
          SHOULD_DELETE=1
          for i in ${STALE_IMAGES_CURRENT[@]}
          do
            echo "$i"
            if [ "$i" = "$DIGEST" ]
            then
              SHOULD_DELETE=0
            fi
          done
        fi

        if [ $SHOULD_DELETE = 0 ]
        then
          echo "Deleting image. Command: gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q"
          gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q || FAILED_TO_DELETE+="${current} "
        fi
      fi
    done
  fi

  # Some images may not be successfully deleted the first time due to flakiness or having a dependency that hasn't been deleted yet.
  RETRY_DELETE=("${FAILED_TO_DELETE[@]}")
  echo "Failed to delete the following images: ${FAILED_TO_DELETE}. Retrying each of them."
  for current in $RETRY_DELETE; do
    echo "Trying again to delete image ${image_name}@"${current}". Command: gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q"
    gcloud container images delete ${image_name}@"${current}" --force-delete-tags -q
  done
done

if [[ ${STALE_IMAGES} ]]; then
  echo "Deleted multiple images"
else
  echo "No stale prebuilt container images found."
fi
