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

sudo chmod 666 /var/run/docker.sock

#escaping env variables to avoid issues in k8s deployment
GITHUB_TOKEN=$(echo "$GITHUB_TOKEN")

registration_url="https://api.github.com/orgs/$ORG_NAME/actions/runners/registration-token"
echo "Requesting registration Org URL at '${registration_url}'"
payload=$(curl -sX POST -H "Authorization: token $GITHUB_TOKEN" ${registration_url})
RUNNER_TOKEN=$(echo $payload | jq '.token' --raw-output)
echo "export RUNNER_TOKEN3=$RUNNER_TOKEN" | sudo tee /etc/bash.bashrc > /dev/null && source /etc/bash.bashrc

./config.sh \
    --name $(hostname) \
    --token ${RUNNER_TOKEN} \
    --url https://github.com/$ORG_NAME \
    --work _work \
    --unattended \
    --replace \
    --labels ubuntu-20.04,ubuntu-latest \
    --runnergroup ${ORG_RUNNER_GROUP}

remove() {
    ./config.sh remove --unattended --token "${RUNNER_TOKEN}"
}
trap 'remove; exit 130' INT
trap 'remove; exit 143' TERM
trap 'remove; exit 143' QUIT
trap 'remove; exit 143' ABRT
trap 'remove; exit 0' EXIT

./run.sh "$*" &
wait $!
