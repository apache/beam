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

# Install Python 3.8 and dependencies
apt-get update > /dev/null
export DEBIAN_FRONTEND=noninteractive
apt-get install -y software-properties-common > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3-pip > /dev/null

cd playground/infrastructure
pip install -r requirements.txt > /dev/null

export \
ORIGIN=PG_EXAMPLES \
STEP=CD \
SUBDIRS="././learning/katas ././examples ././sdks" \
GOOGLE_CLOUD_PROJECT=${PROJECT_ID} \
BEAM_ROOT_DIR="../../" \
SDK_CONFIG="../../playground/sdks.yaml" \
BEAM_EXAMPLE_CATEGORIES="../categories.yaml" \
BEAM_USE_WEBGRPC=yes \
BEAM_CONCURRENCY=4 \
sdks=("java" "python" "go")

# Run CD script to deploy Examples to Playground for Go, Java, Python SDK
for sdk in "${sdks[@]}"
do
  export SERVER_ADDRESS=https://${sdk}.${DNS_NAME}
  python3 ci_cd.py \
  --step ${STEP} \
  --sdk SDK_"${sdk^^}" \
  --origin ${ORIGIN} \
  --subdirs ${SUBDIRS}
done