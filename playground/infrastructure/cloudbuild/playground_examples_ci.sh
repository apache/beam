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

export GRADLE_VERSION=7.5.1
export GO_VERSION=1.18

#Install python java8 and dependencies
apt-get update > /dev/null
export DEBIAN_FRONTEND=noninteractive
apt-get install -y apt-transport-https ca-certificates software-properties-common curl unzip > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3.8-distutils python3-pip > /dev/null
apt install --reinstall python3.8-distutils > /dev/null
pip install --upgrade google-api-python-client > /dev/null
python3.8 -m pip install pip --upgrade > /dev/null
ln -s /usr/bin/python3.8 /usr/bin/python > /dev/null
apt install python3.8-venv > /dev/null

# Install jdk and gradle
apt-get install openjdk-8-jdk -y > /dev/null
curl -L https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -o gradle-${GRADLE_VERSION}-bin.zip > /dev/null
unzip gradle-${GRADLE_VERSION}-bin.zip > /dev/null
export PATH=$PATH:gradle-${GRADLE_VERSION}/bin

# Install go
curl -OL https://golang.org/dl/go$GO_VERSION.linux-amd64.tar.gz > /dev/null
tar -C /usr/local -xvf go$GO_VERSION.linux-amd64.tar.gz > /dev/null
export PATH=$PATH:/usr/local/go/bin

# Install Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" > /dev/null
apt update > /dev/null && apt install -y docker-ce > /dev/null


ls -la && echo "LS -LA"
pwd && echo "PWD"

pip install -r playground/infrastructure/requirements.txt> /dev/null

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
BEAM_VERSION=2.43.0 \
sdks=("go" "java" "python") \
allowlist=(".github/workflows/playground_examples_ci_reusable.yml" \
".github/workflows/playground_examples_ci.yml" \
"playground/backend" \
"playground/infrastructure")

echo "Environment variables exported"

# Get Difference
set -xeu
# define the base ref
base_ref=origin/${BRANCH_NAME}
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
        python3 playground/infrastructure/checker.py \
        --verbose \
        --sdk SDK_"${sdk^^}" \
        --allowlist "${allowpath}" \
        --paths "${diff}"
    done
done
if [[ $? -eq 0 ]]
then
    example_has_changed=True
else
    example_has_changed=False
fi

if [[ ${example_has_changed} == True ]]
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
      if [[ -n "${SDK_TAG}" ]]
      then
            opts="${opts} -Psdk-tag=${SDK_TAG}"
      fi
      for sdk in "${sdks[@]}"
      do
        if [[ "$sdk" == "java" ]]
        then
            # Java uses a fixed BEAM_VERSION
            opts="$opts -Pbase-image=apache/beam_java8_sdk:${BEAM_VERSION}"
        fi
      ./gradlew -i playground:backend:containers:${sdk}:docker ${opts}
      done

      echo "IMAGE_TAG=apache/beam_playground-backend-${sdk}:${DOCKERTAG}" && IMAGE_TAG=apache/beam_playground-backend-${sdk}:${DOCKERTAG}

      set -uex
      NAME=$(docker run -d --rm -p 8080:8080 -e PROTOCOL_TYPE=TCP "$IMAGE_TAG")
      echo "NAME=$NAME" && NAME=$NAME

      for sdk in "${sdks[@]}"
      do
          python3 playground/infrastructure/ci_cd.py \
          --step $STEP \
          --sdk SDK_"${sdk^^}" \
          --origin "${ORIGIN}" \
          --subdirs "${SUBDIRS}"
      done
else
      echo "Example has NOT been changed"
fi