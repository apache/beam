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

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"

   export "$KEY"="$VALUE"
done

export ORIGIN=${ORIGIN-"PG_EXAMPLES"}
export STEP=${STEP-"CD"}
export SUBDIRS=${SUBDIRS-"learning/katas examples sdks"}
export BEAM_ROOT_DIR=${BEAM_ROOT_DIR-"/workspace/beam"}
export PROJECT_ID=${PROJECT_ID}
export SDK_CONFIG=${SDK_CONFIG-"$BEAM_ROOT_DIR/playground/sdks.yaml"}
export BEAM_EXAMPLE_CATEGORIES="$BEAM_ROOT_DIR/playground/categories.yaml"}
export BEAM_USE_WEBGRPC=${BEAM_USE_WEBGRPC-"yes"}
export BEAM_CONCURRENCY=${BEAM_CONCURRENCY-2}
export SDKS=${SDKS-"java python go"}
export COMMIT=${COMMIT-"HEAD"}
export DNS_NAME=${DNS_NAME}

function LogOutput ()
{
    echo "$(date --utc '+%D %T') $1" >> $LOG_PATH
    # CILOG keyword to simplify search over the global log
    echo "CILOG $(date --utc '+%D %T') $1"
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

# Install Python 3.8 and dependencies
apt-get update > /dev/null
apt update > /dev/null
export DEBIAN_FRONTEND=noninteractive

apt-get install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null && apt update > /dev/null
apt install -y python3.8 python3.8-distutils python3-pip > /dev/null
apt install --reinstall python3.8-distutils > /dev/null
pip install --upgrade google-api-python-client > /dev/null
python3.8 -m pip install pip --upgrade > /dev/null
ln -s /usr/bin/python3.8 /usr/bin/python > /dev/null
apt install python3.8-venv > /dev/null
pip install -r /workspace/beam/playground/infrastructure/requirements.txt > /dev/null

LogOutput "Running Examples deployment to ${DNS_NAME}"

# Run CD script to deploy Examples to Playground for Go, Java, Python SDK
for sdk in $SDKS
do
  export SERVER_ADDRESS=https://${sdk}.${DNS_NAME}
  python3 /workspace/beam/playground/infrastructure/ci_cd.py \
  --datastore-project ${PROJECT_ID} \
  --step ${STEP} \
  --sdk SDK_"${sdk^^}" \
  --origin ${ORIGIN} \
  --subdirs ${SUBDIRS}
done

# Note: ${dns.name} taken from cloud build subs