#!/bin/bash

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

# This loop reads the arguments passed to the script, parses them, and exports them as environment variables.
for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"
   export "$KEY"="$VALUE"
done

# This block sets default values for several environment variables to run CD script.
export ORIGIN=${ORIGIN-"PG_EXAMPLES"}
export SUBDIRS=${SUBDIRS-"./learning/katas ./examples ./sdks"}
export BEAM_ROOT_DIR=${BEAM_ROOT_DIR-"/workspace/beam"}
export PROJECT_ID=${PROJECT_ID}
export SDK_CONFIG=${SDK_CONFIG-"$BEAM_ROOT_DIR/playground/sdks.yaml"}
export BEAM_EXAMPLE_CATEGORIES=${BEAM_EXAMPLE_CATEGORIES-"$BEAM_ROOT_DIR/playground/categories.yaml"}
export BEAM_USE_WEBGRPC=${BEAM_USE_WEBGRPC-"yes"}
export BEAM_CONCURRENCY=${BEAM_CONCURRENCY-2}
export SDKS=${SDKS-"java python go"}
export COMMIT=${COMMIT-"HEAD"}
export DNS_NAME=${DNS_NAME}
if [[ -z "${DNS_NAME}" ]]; then
  echo "DNS_NAME is empty or not set. Exiting"
  exit 1
fi
export NAMESPACE=${NAMESPACE}
if [[ -z "${NAMESPACE}" ]]; then
  echo "Datastore NAMESPACE is empty or not set. Exiting"
  exit 1
fi
export SOURCE_BRANCH=${SOURCE_BRANCH}
if [[ -z "${SOURCE_BRANCH}" ]]; then
  echo "PR Source Branch is empty or not set. Exiting"
  exit 1
fi
export ALLOWLIST=${ALLOWLIST-"learning/katas sdks/ examples/"}
export DIFF_BASE=${DIFF_BASE-"origin/master"}

# This function logs the given message to a file and outputs it to the console.
function LogOutput ()
{
    echo "$(date --utc '+%D %T') $1"
    # CDLOG keyword to simplify search over the global log
    echo "CDLOG $(date --utc '+%D %T') $1"
}

LogOutput "Input variables:
 ORIGIN=$ORIGIN
 STEP=${STEP}
 SUBDIRS=$SUBDIRS
 BEAM_ROOT_DIR=$BEAM_ROOT_DIR
 PROJECT_ID=$PROJECT_ID
 SDK_CONFIG=$SDK_CONFIG
 BEAM_EXAMPLE_CATEGORIES=$BEAM_EXAMPLE_CATEGORIES
 BEAM_CONCURRENCY=$BEAM_CONCURRENCY
 SDKS=$SDKS
 COMMIT=$COMMIT
 DNS_NAME=$DNS_NAME
 BEAM_USE_WEBGRPC=$BEAM_USE_WEBGRPC
 NAMESPACE=$NAMESPACE
 SOURCE_BRANCH=$SOURCE_BRANCH"

# Script starts in a clean environment in Cloud Build. Set minimal required environment variables
if [ -z "$PATH" ]; then
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin/:/usr/bin:/sbin:/bin"
fi
if [ -z "$HOME" ]; then
    export HOME="/builder/home"
fi

export STEP=CD

LogOutput "Installing python and dependencies."
# Install Python 3 and dependencies
set -e  # Exit immediately if any command fails
apt update > /dev/null 2>&1
export DEBIAN_FRONTEND=noninteractive
apt install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null 2>&1
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null 2>&1 && apt update > /dev/null 2>&1
apt install -y python3.8 python3.8-distutils python3-pip > /dev/null 2>&1
apt install -y --reinstall python3.8-distutils > /dev/null 2>&1
pip install --upgrade google-api-python-client > /dev/null 2>&1
python3.8 -m pip install pip --upgrade > /dev/null 2>&1
ln -s /usr/bin/python3.8 /usr/bin/python > /dev/null 2>&1
apt install -y python3.8-venv > /dev/null 2>&1
pip install -r /workspace/beam/playground/infrastructure/requirements.txt > /dev/null 2>&1

LogOutput "Python and dependencies have been successfully installed."

LogOutput "Checking what files were changed in the PR."

LogOutput "Looking for changes that require CD validation for [$SDKS] SDKs"
allowlist_array=($ALLOWLIST)
for sdk in $SDKS
do
    result=True
    if [[ $result == True ]]; then
        LogOutput "Running ci_cd.py for SDK $sdk"

        export SERVER_ADDRESS=https://${sdk}.${DNS_NAME}
        python3 ci_cd.py \
        --datastore-project ${PROJECT_ID} \
        --namespace ${NAMESPACE} \
        --step CD \
        --sdk SDK_"${sdk^^}" \
        --origin ${ORIGIN} \
        --subdirs ${SUBDIRS}
        if [ $? -eq 0 ]; then
            LogOutput "Examples for $sdk SDK have been successfully deployed."
        else
            LogOutput "Examples for $sdk SDK were not deployed. Please see the logs."
        fi
    fi
done
