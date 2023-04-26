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

#!/usr/bin/env bash
echo "Search for CILOG keyword to find valuable logs entries"
echo "CILOG $(date --utc '+%D %T') Trigger run inputs:  
    PR URL:$_PR_URL
    PR Source Repo: $_FORK_REPO 
    PR Branch: $_PR_BRANCH
    PRCommit: $_PR_COMMIT
    WebHook Action: $_PR_TYPE
    Merge to: $_BASE_REF"


if [[ "${_BASE_REF}" != "master" ]]; then
    echo "CILOG Merge to branch apache/${_BASE_REF}. Exiting"
    exit 0
fi

if [[ ${_PR_TYPE} == @(opened|synchronize) ]]; then
    
    echo "CILOG See also public logs: ${_PUBLIC_LOG_URL}"

    echo "CILOG $(date --utc '+%D %T') Examples validation (CI) has started"
    
    echo "$(date --utc '+%D %T') Examples validation (CI) has started" >> ${_PUBLIC_LOG_LOCAL}
    
    apt update > /dev/null

    apt-get install -y git curl apt-transport-https ca-certificates gnupg > /dev/null

    echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

    apt-get update && apt-get install -y google-cloud-sdk > /dev/null
    
    curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${_PR_COMMIT} -d '{"state":"pending","target_url":null,"description":"Examples validation (CI) for current commit is in progress","context":"GCP Cloud Build CI/CD"}'

    echo "CILOG $(date --utc '+%D %T') Starting validation script"
    
    echo "$(date --utc '+%D %T') Starting validation script" >> ${_PUBLIC_LOG_LOCAL}
    
    git clone --branch master https://github.com/apache/beam.git
    
    git remote add forked https://github.com/${_FORK_REPO}.git
    
    chmod +x ${_CI_SCRIPT_PATH}
        
    env -i bash -c "${_CI_SCRIPT_PATH} PROJECT_ID='"'${PROJECT_ID}'"' LOG_PATH='"'${_PUBLIC_LOG_LOCAL}'"' BEAM_VERSION='"'${_BEAM_VERSION}'"' COMMIT='"'${_PR_COMMIT}'"' BEAM_CONCURRENCY='"'${_BEAM_CONCURRENCY}'"'"
    
    ci_script_status=$?

    gcloud storage cp ${_PUBLIC_LOG_LOCAL} ${_PUBLIC_BUCKET}
    
    if [ $ci_script_status -eq 0 ]; then
        
        echo "CILOG Writing SUCCESS status message to PR${_PR_NUMBER}, commit:  ${_PR_COMMIT}, branch: ${_PR_BRANCH}"
        
        curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${_PR_COMMIT} -d '{"state":"success","target_url":"${_PUBLIC_LOG_URL}","description":"Examples validation (CI) successfully completed","context":"GCP Cloud Build CI/CD"}'
    else
    
        echo "CILOG Writing FAIL status message to PR${_PR_NUMBER}, commit:  ${_PR_COMMIT}, branch: ${_PR_BRANCH}"
        
        curl -X POST -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $${PAT}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/beam/statuses/${_PR_COMMIT} -d '{"state":"error","target_url":"${_PUBLIC_LOG_URL}","description":"Examples validation has FAILED. For more details please see the logs.","context":"GCP Cloud Build CI/CD"}'
    fi
else
    echo "CILOG $(date --utc '+%D %T') Commit $_PR_COMMIT is not related to any PR"
fi
