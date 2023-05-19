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

echo "Search for CILOG keyword to find valuable logs entries"
echo "CILOG $(date --utc '+%D %T') Trigger run inputs:  
    PR URL:$PR_URL
    PR Source Repo: $FORK_REPO 
    PR Branch: $PR_BRANCH
    PR Number: $PR_NUMBER
    PR Commit: $PR_COMMIT
    WebHook Action: $PR_TYPE
    Merge to: $BASE_REF
    Apache Beam SDK version: $BEAM_VERSION"


if [[ "${BASE_REF}" != "master" ]]; then
            echo "CILOG Merge to branch apache/${BASE_REF}. Exiting"
            exit 0
fi

if [[ ${PR_TYPE} == @(opened|synchronize) ]]; then
    
    echo "CILOG See also public logs: ${PUBLIC_LOG_URL}"

    echo "CILOG $(date --utc '+%D %T') Examples validation (CI) has started"
    
    echo "$(date --utc '+%D %T') Examples validation (CI) has started" >> ${PUBLIC_LOG_LOCAL}
    
    apt update > /dev/null

    apt-get install -y git curl apt-transport-https ca-certificates gnupg > /dev/null

    echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

    apt-get update && apt-get install -y google-cloud-sdk > /dev/null
    
    curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${PR_COMMIT} -d '{"state":"pending","target_url":null,"description":"Examples validation (CI) for current commit is in progress","context":"GCP Cloud Build CI/CD"}'

    echo "CILOG $(date --utc '+%D %T') Starting validation script"
    
    echo "$(date --utc '+%D %T') Starting validation script" >> ${PUBLIC_LOG_LOCAL}
    
    git clone --branch master https://github.com/apache/beam.git
    
    cd beam

    git remote add forked https://github.com/${FORK_REPO}.git

    cd ..
    
    chmod +x ${CI_SCRIPT_PATH}
      
    env -i bash -c "${CI_SCRIPT_PATH} PROJECT_ID=\"${PROJECT_ID}\" LOG_PATH=\"${PUBLIC_LOG_LOCAL}\" BEAM_VERSION=\"${BEAM_VERSION}\" COMMIT=\"${PR_COMMIT}\" "
    
    ci_script_status=$?

    gcloud storage cp ${PUBLIC_LOG_LOCAL} gs://${PUBLIC_BUCKET}
    
    if [ $ci_script_status -eq 0 ]; then

        echo "CILOG Writing SUCCESS status message to PR${PR_NUMBER}, commit:  ${PR_COMMIT}, branch: ${PR_BRANCH}"

        curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${PR_COMMIT} -d "{\"state\":\"success\",\"target_url\":\"${PUBLIC_LOG_URL}\",\"description\":\"Examples validation (CI) successfully completed\",\"context\":\"GCP Cloud Build CI/CD\"}"
    else
    
        echo "CILOG Writing FAIL status message to PR${PR_NUMBER}, commit:  ${PR_COMMIT}, branch: ${PR_BRANCH}"

        curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${PR_COMMIT} -d "{\"state\":\"error\",\"target_url\":\"${PUBLIC_LOG_URL}\",\"description\":\"Examples validation has FAILED. For more details please see the logs.\",\"context\":\"GCP Cloud Build CI/CD\"}"
    fi
else
    echo "CILOG $(date --utc '+%D %T') Commit $PR_COMMIT is not related to any PR"
fi
