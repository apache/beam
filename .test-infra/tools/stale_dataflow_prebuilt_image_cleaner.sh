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

STALE_IMAGES=$(gcloud container images list-tags \
gcr.io/apache-beam-testing/prebuilt_beam_sdk/beam_python_prebuilt_sdk \
--sort-by=TIMESTAMP  --filter="timestamp.datetime < $(date --iso-8601=s -d '1 day ago')" \
--format="get(digest)")

if [[ ${STALE_IMAGES} ]]; then
  for digest in ${STALE_IMAGES}; do
    gcloud container images delete gcr.io/apache-beam-testing/prebuilt_beam_sdk/beam_python_prebuilt_sdk@"$digest" --force-delete-tags -q
  done
else
  echo "No stale prebuilt container images found."
fi
