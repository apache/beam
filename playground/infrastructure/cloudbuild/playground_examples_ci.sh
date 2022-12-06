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

#Install python java8 and dependencies
apt-get update > /dev/null
export DEBIAN_FRONTEND=noninteractive
apt-get install -y software-properties-common git > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3-pip > /dev/null
# Install jdk
apt-get install openjdk-11-jdk -y > /dev/null

cd playground/infrastructure
pip install -r requirements.txt > /dev/null

echo "Dependencies installed"

export \
ORIGIN=PG_EXAMPLES \
STEP=CI \
SUBDIRS="././learning/katas ././examples ././sdks" \
GOOGLE_CLOUD_PROJECT=$PROJECT_ID \
BEAM_ROOT_DIR="../../" \
SDK_CONFIG="../../playground/sdks.yaml" \
BEAM_EXAMPLE_CATEGORIES="../categories.yaml" \
BEAM_CONCURRENCY=4 \
SERVER_ADDRESS=localhost:8080 \
sdks=("go" "java" "python") \
allowlist=(".github/workflows/playground_examples_ci_reusable.yml" \
".github/workflows/playground_examples_ci.yml" \
"playground/backend" \
"playground/infrastructure")

echo "Environment variables exported"

# Get Difference
set -xeu
# define the base ref
base_ref=${BRANCH_NAME}
if [[ -z "$base_ref" ]] || [[ "$base_ref" == "master" ]]
then
  base_ref=origin/master
fi
diff=$(git diff --name-only $base_ref "${COMMIT_SHA}" | tr '\n' ' ')

# Check if there are Examples
set +e -ux
for sdk in "${sdks[@]}"
do
    for allowpath in "${allowlist[@]}"
    do
        python3 checker.py \
        --verbose \
        --sdk SDK_"${sdk^^}" \
        --allowlist "${allowpath}" \
        --paths "${diff}"
    done
done
if [[ $? -eq 0 ]]
then
    example_has_changed=True
    echo "Example has been changed"
else
    example_has_changed=False
    echo "Example has NOT been changed"
fi

# If there are changed examples run CI scripts
if [[ "$example_has_changed" == True ]]
then
    rm ~/.m2/settings.xml

    if [[ -z ${TAG_NAME} ]]
    then
        DOCKERTAG=${COMMIT_SHA} && echo "DOCKERTAG=${COMMIT_SHA}"
    elif [[ -z ${COMMIT_SHA} ]]
    then
        DOCKERTAG=${TAG_NAME} && echo "DOCKERTAG=${TAG_NAME}"
    fi

    set -uex
    for sdk in "${sdks[@]}"
    do
      if [[ "$sdk" == "python" ]]
      then
          # builds apache/beam_python3.7_sdk:$DOCKERTAG image
          ./gradlew -i :sdks:python:container:py37:docker -Pdocker-tag="$DOCKERTAG"
          # and set SDK_TAG to DOCKERTAG so that the next step would find it
          echo "SDK_TAG=${DOCKERTAG}" && SDK_TAG=${DOCKERTAG}
      fi
    done

    set -ex
    opts=" -Pdocker-tag=$DOCKERTAG"
    if [[ -n "$SDK_TAG" ]]
    then
        opts="$opts -Psdk-tag=$SDK_TAG"
    fi
    if [[ "$SDK" == "java" ]]
    then
        # Java uses a fixed BEAM_VERSION
        opts="$opts -Pbase-image=apache/beam_java8_sdk:$BEAM_VERSION"
    fi

    # by default (w/o -Psdk-tag) runner uses BEAM from local ./sdks
    # TODO Java SDK doesn't, it uses 2.42.0, fix this
    ./gradlew -i playground:backend:containers:"$SDK":docker "$opts"

    echo "IMAGE_TAG=apache/beam_playground-backend-$SDK:$DOCKERTAG" && IMAGE_TAG=apache/beam_playground-backend-$SDK:$DOCKERTAG

    set -uex
    NAME=$(docker run -d --rm -p 8080:8080 -e PROTOCOL_TYPE=TCP "$IMAGE_TAG")
    echo "NAME=$NAME" && NAME=$NAME

    for sdk in "${sdks[@]}"
    do
      python3 ci_cd.py \
      --step $STEP \
      --sdk SDK_"${sdk^^}" \
      --origin "${ORIGIN}" \
      --subdirs "${SUBDIRS}"
    done
fi