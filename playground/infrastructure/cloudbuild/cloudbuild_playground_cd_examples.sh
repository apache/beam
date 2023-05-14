#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

echo "Search for CDLOG keyword to find valuable logs entries"

echo "CDLOG $(date --utc '+%D %T') Trigger run inputs:
    PR URL: $PR_URL
    Merged to: $TARGET_PR_REPO_BRANCH
    WebHook Action: $PR_TYPE
    Merge Status: $MERGE_STATUS
    Merge commit: $MERGE_COMMIT
    Playground DNS: $DNS_NAME
    Datastore namespace: $DATASTORE_NAMESPACE"

if [[ -z "$DNS_NAME" || -z "$DATASTORE_NAMESPACE" ]]; then
    echo "DNS_NAME and DATASTORE_NAMESPACE substitutions must be set in the triger"
    exit 1
fi

if [[ ${TARGET_PR_REPO_BRANCH} != "apache:master" ]]; then
    echo "CDLOG Merging not into the master, but into the branch apache/${TARGET_PR_REPO_BRANCH}. Exiting"
    exit 0
fi

if [[ ${PR_TYPE} != "closed" ]] || [[ ${MERGE_STATUS} != "true" ]];
then
    echo "CDLOG $(date --utc '+%D %T') PR in $PR_URL for the COMMIT $MERGE_COMMIT is not in closed state or not merged into the Apache/Beam master repo."
    exit 0
fi

echo "CDLOG Pull Request $PR_URL has been successfully merged into
Apache Beam GitHub repository. Continuing the process."
  
echo "CDLOG Continous Deployment of Playground Examples (CD) in the
progress."

apt update > /dev/null 2>&1

apt install -y git curl > /dev/null 2>&1

apt-get install -y apt-transport-https ca-certificates gnupg > /dev/null 2>&1

echo "deb https://packages.cloud.google.com/apt cloud-sdk main" > /dev/null 2>&1 | tee -a

/etc/apt/sources.list.d/google-cloud-sdk.list > /dev/null 2>&1

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg > /dev/null 2>&1 | apt-key add - > /dev/null 2>&1 

apt-get update && apt-get install -y google-cloud-sdk > /dev/null 2>&1

echo "CDLOG $(date --utc '+%D %T') Starting deployment script"

git clone --branch master https://github.com/apache/beam.git

chmod +x ${CD_SCRIPT_PATH}

env -i bash -c "${CD_SCRIPT_PATH} \
DATASTORE_NAMESPACE=\"${DATASTORE_NAMESPACE}\" \
DNS_NAME=\"${DNS_NAME}\" PROJECT_ID=\"${PROJECT_ID}\" \
MERGE_COMMIT=\"${MERGE_COMMIT}\" FORCE_CD=\"${FORCE_CD}\" \
ORIGIN=\"${ORIGIN}\" SUBDIRS=\"${SUBDIRS}\" SDKS=\"${SDKS}\" \
BEAM_CONCURRENCY=\"${BEAM_CONCURRENCY}\" "

cd_script_status=$?

if [ $cd_script_status -eq 0 ]; then
    echo "CDLOG $(date --utc '+%D %T') Examples deployment has been successfully completed. Please check Playground DNS https://${DNS_NAME}."
else
    echo "CDLOG $(date --utc '+%D %T') Examples deployment has failed. Please check the Cloud Build logs."
fi