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

# Script validates examples against runners (Java, Python, Go) built as local containers from a current branch
#
# Command line arguments:
# LOG_PATH - full path lo a log file. Output is also logged to stdout (Default: /dev/null)
# BEAM_ROOT_DIR - Path (full) to cloned repo root. Used by ci_cd.py (Default: /workspace/beam)
# PROJECT_ID - GCP Project ID. Used by ci_cd.py (Default: test)
# BEAM_CONCURRENCY - Number of examples to run in parallel. Used by ci_cd.py (Default: 4)
# BEAM_VERSION - version of BEAM SDK to build containers
# SUBDIRS - array of paths (relative to beam repo root) to search changed examples (Default: ./learning/katas ./examples ./sdks)
# COMMIT - Git commit hash to build containers from (Default: HEAD)
# DIFF_BASE - Git branch to compare with $COMMIT to look for changed examples (Default: origin/master)
# SDKS - Array of SDKS to validate (Default: java python go)
# ORIGIN - examples origin (Default: PG_EXAMPLES)
# ALLOWLIST - List of paths (relative to the repo root) not in examples (SUBDIRS) that cause reb

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"

   export "$KEY"="$VALUE"
done

export LOG_PATH=${LOG_PATH-"/dev/null"}
export BEAM_ROOT_DIR=${BEAM_ROOT_DIR-"/workspace/beam"}
export PROJECT_ID=${PROJECT_ID-"test"}
export BEAM_VERSION=${BEAM_VERSION-"2.44.0"}
export SUBDIRS=${SUBDIRS-"./learning/katas ./examples ./sdks"}
export SDKS=${SDKS-"java python go"}
export COMMIT=${COMMIT-"HEAD"}
export DIFF_BASE=${DIFF_BASE-"origin/master"}
export BEAM_CONCURRENCY=${BEAM_CONCURRENCY-"4"}
export ORIGIN=${ORIGIN-"PG_EXAMPLES"}
export ALLOWLIST=${ALLOWLIST-"playground/infrastructure playground/backend"}

function LogOutput ()
{
    echo "$(date --utc '+%D %T') $1" >> $LOG_PATH
    # CILOG keyword to simplify search over the global log
    echo "CILOG $(date --utc '+%D %T') $1"
}


LogOutput "Input variables:
            LOG_PATH=$LOG_PATH
            BEAM_ROOT_DIR=$BEAM_ROOT_DIR
            PROJECT_ID=$PROJECT_ID
            BEAM_VERSION=$BEAM_VERSION
            SUBDIRS=$SUBDIRS
            SDKS=$SDKS
            COMMIT=$COMMIT
            BEAM_CONCURRENCY=$BEAM_CONCURRENCY
            ORIGIN=$ORIGIN
            ALLOWLIST=$ALLOWLIST"

# Assigning constant values
# Script starts in a clean environment in Cloud Build. Set minimal required environment variables
if [ -z "$PATH" ]; then
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin/:/usr/bin:/sbin:/bin"
fi
if [ -z "$HOME" ]; then
    export HOME="/builder/home"
fi
export STEP=CI 
export SDK_CONFIG="$BEAM_ROOT_DIR/playground/sdks.yaml"
export BEAM_EXAMPLE_CATEGORIES="$BEAM_ROOT_DIR/playground/categories.yaml"
export GRADLE_VERSION=7.5.1
export GO_VERSION=1.23.1

LogOutput "Installing python java8 and dependencies"
apt-get update > /dev/null
apt update > /dev/null
export DEBIAN_FRONTEND=noninteractive

LogOutput "Installing Python environment"
apt-get install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3.8-distutils python3-pip > /dev/null
apt install --reinstall python3.8-distutils > /dev/null
apt install -y python3-googleapi > /dev/null
python3.8 -m pip install pip --upgrade > /dev/null
ln -s /usr/bin/python3.8 /usr/bin/python > /dev/null
apt install python3.8-venv python3.12-venv > /dev/null
python -m venv /tmp/venv
source /tmp/venv/bin/activate
LogOutput "Installing Python packages from beam/playground/infrastructure/requirements.txt"
pip install -r $BEAM_ROOT_DIR/playground/infrastructure/requirements.txt

LogOutput "Installing JDK and Gradle"
apt-get install openjdk-8-jdk -y > /dev/null
curl -L https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -o gradle-${GRADLE_VERSION}-bin.zip > /dev/null
unzip gradle-${GRADLE_VERSION}-bin.zip > /dev/null
export PATH=$PATH:gradle-${GRADLE_VERSION}/bin > /dev/null

LogOutput "Installing GO"
curl -OL https://golang.org/dl/go$GO_VERSION.linux-amd64.tar.gz > /dev/null
tar -C /usr/local -xvf go$GO_VERSION.linux-amd64.tar.gz > /dev/null
export PATH=$PATH:/usr/local/go/bin > /dev/null
# required to build beam/sdks/go container
export GOMODCACHE="$HOME/go/pkg/mod"

LogOutput "Installing Docker"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - > /dev/null
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" > /dev/null
apt update > /dev/null && apt install -y docker-ce > /dev/null

tag_name=$(git tag --points-at $COMMIT)
if [ "$tag_name" ]
then
    LogOutput "Commit $COMMIT is tagged with $tag_name"
    DOCKERTAG=$tag_name
else
    LogOutput "Commit $COMMIT is not tagged"
    DOCKERTAG=$COMMIT
fi
LogOutput "Docker tag for containers: $DOCKERTAG"

cd $BEAM_ROOT_DIR
LogOutput "git fetch --all"
git fetch --all
# Docker containers will build from the current PR commit
LogOutput "git checkout $COMMIT"
git checkout $COMMIT
if [ $? -ne 0 ]; then
   LogOutput "Can't checkout to $COMMIT. Exiting script"
   exit 1 
fi

diff_log=$(git diff --name-only $DIFF_BASE...$COMMIT)
diff=($(echo "$diff_log" | tr '\n' ' '))
LogOutput "Discovered changes introduced by $COMMIT relative to $DIFF_BASE in files:
$diff_log"

LogOutput "Looking for changes that require CI validation for [$SDKS] SDKs"
allowlist_array=($ALLOWLIST)
for sdk in $SDKS
do
    eval "ci_${sdk}_passed"='False'
    example_has_changed="UNKNOWN"
    LogOutput "------------------Starting checker.py for SDK_${sdk^^}------------------"    
    cd $BEAM_ROOT_DIR/playground/infrastructure
    python3 checker.py \
    --verbose \
    --sdk SDK_"${sdk^^}" \
    --allowlist "${allowlist_array[@]}" \
    --paths "${diff[@]}"  >> ${LOG_PATH} 2>&1
    checker_status=$?
    cd $BEAM_ROOT_DIR
    if [ $checker_status -eq 0 ]
    then
        LogOutput "Checker found changed examples for SDK_${sdk^^} or changes in allowlist: $ALLOWLIST"
        example_has_changed=True
    elif [ $checker_status -eq 11 ]
    then
        LogOutput "Checker did not find any changed examples for SDK_${sdk^^}"
        example_has_changed=False
    else
        LogOutput "Error: Checker is broken. Exiting the script."
        exit 1
    fi

    #Nothing to check
    if [[ $example_has_changed != "True" ]]
    then
        LogOutput "No changes require validation for SDK_${sdk^^}"
        eval "ci_${sdk}_passed"='True'
        continue
    fi

    # docker_options="-Psdk-tag=${DOCKERTAG}"
    sdk_tag=$BEAM_VERSION

    # Special cases for Python and Java
    if [ "$sdk" == "python" ]
    then
        # Build fails without docker-pull-licenses=true in Cloud Build
        LogOutput "Building Python base image container apache/beam_python3.10_sdk:$DOCKERTAG"
        LogOutput "./gradlew -i :sdks:python:container:py310:docker -Pdocker-tag=$DOCKERTAG -Pdocker-pull-licenses=true"
        sdk_tag=$DOCKERTAG
        ./gradlew -i :sdks:python:container:py310:docker -Pdocker-tag=$DOCKERTAG -Pdocker-pull-licenses=true
        if [ $? -ne 0 ]
        then
            LogOutput "Build failed for apache/beam_python3.10_sdk:$DOCKERTAG"
            continue
        fi
    fi

    LogOutput "Buidling a container for $sdk runner"
    LogOutput "./gradlew -i playground:backend:containers:"${sdk}":docker -Psdk-tag=$sdk_tag -Pdocker-tag=$DOCKERTAG"
    ./gradlew -i playground:backend:containers:"${sdk}":docker -Psdk-tag=$sdk_tag -Pdocker-tag=$DOCKERTAG
    if [ $? -ne 0 ]
    then
        LogOutput "Container build failed for $sdk runner"
        continue
    fi
    LogOutput "Starting container for $sdk runner"
    docker run -d -p 8080:8080 --network=cloudbuild -e PROTOCOL_TYPE=TCP --name container-${sdk} apache/beam_playground-backend-${sdk}:$DOCKERTAG
    sleep 10
    export SERVER_ADDRESS=container-${sdk}:8080

    LogOutput "Starting ci_cd.py to validate ${sdk} examples"
    cd $BEAM_ROOT_DIR/playground/infrastructure
    python3 ci_cd.py \
    --step ${STEP} \
    --sdk SDK_"${sdk^^}" \
    --origin ${ORIGIN} \
    --subdirs ${SUBDIRS} >> ${LOG_PATH} 2>&1
    if [ $? -eq 0 ]
    then
        LogOutput "Example validation for $sdk SDK successfully completed"
        eval "ci_${sdk}_passed"='True'
    else
        LogOutput "Example validation for $sdk SDK failed"
    fi
    cd $BEAM_ROOT_DIR
    LogOutput "Stopping container for $sdk runner"
    docker stop container-${sdk} > /dev/null
    docker rm container-${sdk} > /dev/null
done

LogOutput "Script finished"
for sdk in $SDKS
do
    result=$(eval echo '$'"ci_${sdk}_passed")
    if [ "$result" != "True" ]; then
        LogOutput "At least one of the checks has failed for $sdk SDK"
        exit 1
    fi
done
exit 0
