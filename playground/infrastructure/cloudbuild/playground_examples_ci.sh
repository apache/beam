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
apt update > /dev/null
export DEBIAN_FRONTEND=noninteractive
apt install -y git > /dev/null

git fetch

apt-get install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3.8-distutils python3-pip > /dev/null
apt install --reinstall python3.8-distutils > /dev/null
pip install --upgrade google-api-python-client > /dev/null
python3.8 -m pip install pip --upgrade > /dev/null
ln -s /usr/bin/python3.8 /usr/bin/python > /dev/null
apt install python3.8-venv > /dev/null
pip install -r playground/infrastructure/requirements.txt > /dev/null

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
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - > /dev/null
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" > /dev/null
apt update 2>/dev/null | grep packages | cut -d '.' -f 1 && apt install -y docker-ce > /dev/null

export \
ORIGIN=PG_EXAMPLES \
STEP=CI \
SUBDIRS="./learning/katas ./examples ./sdks" \
GOOGLE_CLOUD_PROJECT=$PROJECT_ID \
BEAM_ROOT_DIR="." \
SDK_CONFIG="playground/sdks.yaml" \
BEAM_EXAMPLE_CATEGORIES="playground/categories.yaml" \
BEAM_CONCURRENCY=4 \
BEAM_VERSION=2.43.0 \
sdks=("java" "python" "go") \
allowlist="playground/infrastructure playground/backend \
playground/infrastructure/cloudbuild/cloudbuild_examples_ci_steps.yaml \
playground/infrastructure/cloudbuild/playground_examples_ci.sh"

# Get Difference

echo $BRANCH_NAME
git branch
git checkout origin/${BRANCH_NAME}

git branch

base_ref=origin/${BRANCH_NAME}
if [ -z "$base_ref" ] || [ "$base_ref" == "refs/heads/master" ]
then
  base_ref=refs/heads/master
fi
diff=$(git diff --name-only origin/${BRANCH_NAME})

echo ${diff}
# Check if there are Examples
for sdk in "${sdks[@]}"
do
      python3 playground/infrastructure/checker.py \
      --sdk SDK_"${sdk^^}" \
      --allowlist "${allowlist}" \
      --paths "${diff}"
      if [ $? -eq 0 ]
      then
          example_has_changed=True
      else
          example_has_changed=False
      fi

      if [[ ${example_has_changed} == True ]]
      then
            if [ "$sdk" == "python" ]
            then
                # builds apache/beam_python3.7_sdk:$DOCKERTAG image
                ./gradlew -i :sdks:python:container:py37:docker -Pdocker-tag=${DOCKERTAG}
                # and set SDK_TAG to DOCKERTAG so that the next step would find it
                SDK_TAG=${DOCKERTAG}
            else
                unset SDK_TAG
            fi

            echo "SDK_TAG for ${sdk} - ${SDK_TAG}"
            opts=" -Pdocker-tag=${DOCKERTAG}"
            if [ -n "$SDK_TAG" ]; then
                opts="${opts} -Psdk-tag=${SDK_TAG}"
            fi

            if [ "$sdk" == "java" ]
            then
                # Java uses a fixed BEAM_VERSION
                opts="$opts -Pbase-image=apache/beam_java8_sdk:${BEAM_VERSION}"
            fi

            ./gradlew -i playground:backend:containers:${sdk}:docker ${opts} -Pdocker-repository-root="us-central1-docker.pkg.dev/sandbox-playground-008/playground-repository"

            docker push us-central1-docker.pkg.dev/sandbox-playground-008/playground-repository/beam_playground-backend-${sdk}:${DOCKERTAG}
            docker run -d -p 8080:8080 --network=cloudbuild -e PROTOCOL_TYPE=TCP --name container-${sdk} us-central1-docker.pkg.dev/sandbox-playground-008/playground-repository/beam_playground-backend-${sdk}:${DOCKERTAG}
            sleep 10
            export SERVER_ADDRESS=container-${sdk}:8080
            python3 playground/infrastructure/ci_cd.py \
            --step ${STEP} \
            --sdk SDK_"${sdk^^}" \
            --origin ${ORIGIN} \
            --subdirs ${SUBDIRS}

            docker stop container-${sdk}
            docker rm container-${sdk}
            sleep 10
      else
          echo "Example has NOT been changed"
      fi
done