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

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"
   export "$KEY"="$VALUE"
done

export LOG_PATH=${LOG_PATH-"/dev/null"}
export ORIGIN=${ORIGIN-"PG_EXAMPLES"}
export STEP=${STEP-"CD"}
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

function LogOutput ()
{
    echo "$(date --utc '+%D %T') $1" >> $LOG_PATH
    # CILOG keyword to simplify search over the global log
    echo "CDLOG $(date --utc '+%D %T') $1"
}

LogOutput "Input variables:
ORIGIN=$ORIGIN
STEP=$STEP
SUBDIRS=$SUBDIRS
BEAM_ROOT_DIR=$BEAM_ROOT_DIR
PROJECT_ID=$PROJECT_ID
SDK_CONFIG=$SDK_CONFIG
BEAM_EXAMPLE_CATEGORIES=$BEAM_EXAMPLE_CATEGORIES
BEAM_CONCURRENCY=$BEAM_CONCURRENCY
SDKS=$SDKS
COMMIT=$COMMIT
DNS_NAME=$DNS_NAME
BEAM_USE_WEBGRPC=$BEAM_USE_WEBGRPC"

# Script starts in a clean environment in Cloud Build. Set minimal required environment variables
if [ -z "$PATH" ]; then
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin/:/usr/bin:/sbin:/bin"
fi
if [ -z "$HOME" ]; then
    export HOME="/builder/home"
fi

export STEP=CD
export SDK_CONFIG="$BEAM_ROOT_DIR/playground/sdks.yaml"
export BEAM_EXAMPLE_CATEGORIES="$BEAM_ROOT_DIR/playground/categories.yaml"
export DNS_NAME=${DNS_NAME}

LogOutput "Installing python and dependencies"
# Install Python 3 and dependencies
set -e  # Exit immediately if any command fails
apt update > /dev/null 2>&1
apt install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null 2>&1
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null 2>&1
apt update > /dev/null 2>&1
apt install -y python3 python3-distutils python3-pip python3-venv > /dev/null 2>&1
python3 -m pip install --upgrade pip google-api-python-client > /dev/null 2>&1
ln -s /usr/bin/python3 /usr/bin/python > /dev/null 2>&1
pip install -r /workspace/beam/playground/infrastructure/requirements.txt > /dev/null 2>&1

# Check if Python and pip commands are available
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed or not in PATH. Aborting."
    exit 1
fi
if ! command -v pip &> /dev/null; then
    echo "pip is not installed or not in PATH. Aborting."
    exit 1
fi

LogOutput "Running Examples deployment to ${DNS_NAME}"

# Run CD script to deploy Examples to Playground for Go, Java, Python SDK
cd $BEAM_ROOT_DIR/playground/infrastructure
for sdk in $SDKS
do
  export SERVER_ADDRESS=https://${sdk}.${DNS_NAME}
  python3 ci_cd.py \
  --datastore-project test-tob-deploy-001 \
  --step ${STEP} \
  --sdk SDK_"${sdk^^}" \
  --origin ${ORIGIN} \
  --subdirs ${SUBDIRS}
done

# Note: ${dns.name} taken from cloud build subs