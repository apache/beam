#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Verify docker and docker compose installation
docker -v && docker compose version

# Verify gcloud installation
gcloud version

# Activate the mounted Service Account.
gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}" --no-user-output-enabled

# Create gcloud docker config
gcloud auth configure-docker -q --no-user-output-enabled >& /dev/null

# Register the Runner Token in the given Organization.
CLOUD_FUNCTION_URL="https://$GCP_REGION-$GCP_PROJECT_ID.cloudfunctions.net/$CLOUD_FUNCTION_NAME"

# Get the Runner token from the specified Google Cloud Function
response=$(curl -sX POST -H "Authorization:bearer $(gcloud auth print-identity-token)" ${CLOUD_FUNCTION_URL})
RUNNER_TOKEN=$(echo "$response" | jq '.token' --raw-output)

./config.sh \
    --name $(hostname) \
    --token ${RUNNER_TOKEN} \
    --url https://github.com/$ORG_NAME \
    --work _work \
    --unattended \
    --replace \
    --labels ubuntu-20.04,beam \
    --runnergroup ${ORG_RUNNER_GROUP} \
    --ephemeral

remove() {
    ./config.sh remove --token "${RUNNER_TOKEN}"
}
trap 'remove; exit 130' INT
trap 'remove; exit 143' TERM
trap 'remove; exit 143' QUIT
trap 'remove; exit 143' ABRT
trap 'remove; exit 0' EXIT

./run.sh "$*" &
wait $!
