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

# Curl command to create commit status
# Some comment & more etc. ..
curl \
-X POST \
-H "Accept: application/vnd.github+json" \
-H "Authorization: Bearer ${PAT}" \
-H "X-GitHub-Api-Version: 2022-11-28" \
https://api.github.com/repos/apache/beam/statuses/${commit_sha} \
-d '{"state":"success","target_url":"https://console.cloud.google.com/cloud-build/builds;region='${LOCATION}'/'${BUILD_ID}'?authuser=3&project='${PROJECT_ID}'","description":"Successful Examples validation (CI) for current branch","context":"Cloud Build CI/CD"}'
