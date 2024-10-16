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
export FORCE_CD=${FORCE_CD-'false'}

# Playground FQDN
if [[ -z "${DNS_NAME}" ]]; then
  echo "DNS_NAME is empty or not set. Exiting"
  exit 1
fi
# GCP Datastore namespace to load examples to
if [[ -z "${DATASTORE_NAMESPACE}" ]]; then
  echo "DATASTORE_NAMESPACE parameter was not set. Exiting"
  exit 1
fi
# Master branch commit after merge
if [[ -z "${MERGE_COMMIT}" ]]; then
  echo "MERGE_COMMIT paramter was not set. Exiting"
  exit 1
fi
if [[ "${FORCE_CD}" != "false" &&  "${FORCE_CD}" != "true" ]]; then
  echo "FORCE_CD parameter must be either 'true' or 'false'. Exiting"
  exit 1
fi

# Constants
export STEP=CD

# This function logs the given message to a file and outputs it to the console.
function LogOutput ()
{
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
 MERGE_COMMIT=$MERGE_COMMIT
 DNS_NAME=$DNS_NAME
 BEAM_USE_WEBGRPC=$BEAM_USE_WEBGRPC
 DATASTORE_NAMESPACE=$DATASTORE_NAMESPACE
 FORCE_CD=$FORCE_CD"
 

# Script starts in a clean environment in Cloud Build. Set minimal required environment variables
if [ -z "$PATH" ]; then
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin/:/usr/bin:/sbin:/bin"
fi
if [ -z "$HOME" ]; then
    export HOME="/builder/home"
fi

LogOutput "Installing python and dependencies."
# set -e  # Exit immediately if any command fails
export DEBIAN_FRONTEND=noninteractive
apt install -y apt-transport-https ca-certificates software-properties-common curl unzip apt-utils > /dev/null 2>&1
add-apt-repository -y ppa:deadsnakes/ppa > /dev/null 2>&1 && apt update > /dev/null 2>&1
apt install -y python3.9 python3-distutils python3-pip > /dev/null 2>&1
apt install -y --reinstall python3-distutils > /dev/null 2>&1
apt install -y python3-virtualenv
virtualenv play_venv
source play_venv/bin/activate
pip install --upgrade google-api-python-client > /dev/null 2>&1
python3.9 -m pip install pip --upgrade > /dev/null 2>&1
ln -s /usr/bin/python3.9 /usr/bin/python > /dev/null 2>&1
apt install -y python3.9-venv > /dev/null 2>&1

LogOutput "Installing Python packages from beam/playground/infrastructure/requirements.txt"
cd $BEAM_ROOT_DIR
pip install -r playground/infrastructure/requirements.txt

LogOutput "Looking for files changed by the merge commit $MERGE_COMMIT"
git fetch origin $MERGE_COMMIT
diff_log=$(git diff --name-only $MERGE_COMMIT~ $MERGE_COMMIT)
diff=($(echo "$diff_log" | tr '\n' ' '))
LogOutput "Discovered changes introduced by $MERGE_COMMIT: $diff_log"
git checkout $MERGE_COMMIT
if [ $? -ne 0 ]; then
    LogOutput "Can't checkout to $MERGE_COMMIT. Exiting"
    exit 1
fi
declare -a allowlist_array
for sdk in $SDKS
do
    eval "check_${sdk}_passed"="false"
    example_has_changed="UNKNOWN"
    if [ "$FORCE_CD" = "true" ]; then
        LogOutput "FORCE_CD is true. Example deployment for SDK_${sdk^^} is forced"
        example_has_changed="true"
    else
        LogOutput "------------------Starting checker.py for SDK_${sdk^^}------------------"    
        cd $BEAM_ROOT_DIR/playground/infrastructure
        python3 checker.py \
        --verbose \
        --sdk SDK_"${sdk^^}" \
        --allowlist "${allowlist_array[@]}" \
        --paths "${diff[@]}"

        checker_status=$?
        if [ $checker_status -eq 0 ]
        then
            LogOutput "Checker found changed examples for SDK_${sdk^^}"
            example_has_changed="true"
        elif [ $checker_status -eq 11 ]
        then
            LogOutput "Checker did not find any changed examples for SDK_${sdk^^}"
            example_has_changed="false"
        else
            LogOutput "Error: Checker is broken. Exiting the script."
            exit 1
        fi
    fi
    #Nothing to check
    if [[ $example_has_changed != "true" ]]
    then
        LogOutput "No changes require validation for SDK_${sdk^^}"
        eval "check_${sdk}_passed"="true"
        continue
    fi
    
    cd $BEAM_ROOT_DIR/playground/infrastructure
    LogOutput "Running ci_cd.py for SDK $sdk"

    export SERVER_ADDRESS=https://${sdk}.${DNS_NAME}
    python3 ci_cd.py \
    --datastore-project ${PROJECT_ID} \
    --namespace ${DATASTORE_NAMESPACE} \
    --step ${STEP} \
    --sdk SDK_"${sdk^^}" \
    --origin ${ORIGIN} \
    --subdirs ${SUBDIRS}
    if [ $? -eq 0 ]; then
        LogOutput "Examples for $sdk SDK have been successfully deployed."
        eval "check_${sdk}_passed"="true"
    else
        LogOutput "Examples for $sdk SDK were not deployed. Please see the logs."
    fi
done
LogOutput "Script finished"
for sdk in $SDKS
do
    result=$(eval echo '$'"check_${sdk}_passed")
    if [ "$result" != "true" ]; then
        LogOutput "At least one of the checks has failed for $sdk SDK"
        exit 1
    fi
done
exit 0