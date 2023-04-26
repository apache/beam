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



locals { 
    cloudbuild_init_environment = [ 
    "REPO_NAME=$_REPO_NAME" ,
    "BRANCH_NAME=$_BRANCH_NAME",
    "ENVIRONMENT_NAME=$_ENVIRONMENT_NAME",
    "TF_VAR_project_id=$PROJECT_ID",
    "TF_VAR_network_name=$_NETWORK_NAME",
    "TF_VAR_subnetwork_name=$_SUBNETWORK_NAME",
    "TF_VAR_gke_name=$_GKE_NAME",
    "TF_VAR_region=$_PLAYGROUND_REGION",
    "TF_VAR_zone=$_PLAYGROUND_ZONE",
    "TF_VAR_state_bucket=$_STATE_BUCKET",
    "TF_VAR_redis_name=$_REDIS_NAME",
    "TF_VAR_redis_tier=$_REDIS_TIER",
    "TF_VAR_min_count=$_MIN_COUNT",
    "TF_VAR_max_count=$_MAX_COUNT",
    "TF_VAR_skip_appengine_deploy=$_SKIP_APPENGINE_DEPLOY",
    "TF_VAR_ip_address_name=$_IPADDRESS_NAME",
    "TF_VAR_repository_id=$_REPOSITORY_NAME",
    "TF_VAR_service_account_id=$_SERVICEACCOUNT_ID",
    "TF_VAR_gke_machine_type=$_GKE_MACHINE_TYPE"] 
    
  
    cloudbuild_deploy_environment = [ 
    "REPO_NAME=$_REPO_NAME" ,
    "BRANCH_NAME=$_BRANCH_NAME",
    "ENVIRONMENT_NAME=$_ENVIRONMENT_NAME",
    "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
    "DNS_NAME=$_DNS_NAME",
    "SDK_TAG=$_SDK_TAG",
    "CONTAINER_TAG=$_CONTAINER_TAG",
    "TF_VAR_project_id=$PROJECT_ID",
    "TF_VAR_network_name=$_NETWORK_NAME",
    "TF_VAR_subnetwork_name=$_SUBNETWORK_NAME",
    "TF_VAR_gke_name=$_GKE_NAME",
    "TF_VAR_region=$_PLAYGROUND_REGION",
    "TF_VAR_zone=$_PLAYGROUND_ZONE",
    "TF_VAR_state_bucket=$_STATE_BUCKET",
    "TF_VAR_redis_name=$_REDIS_NAME",
    "TF_VAR_redis_tier=$_REDIS_TIER",
    "TF_VAR_min_count=$_MIN_COUNT",
    "TF_VAR_max_count=$_MAX_COUNT",
    "TF_VAR_skip_appengine_deploy=$_SKIP_APPENGINE_DEPLOY",
    "TF_VAR_ip_address_name=$_IPADDRESS_NAME",
    "TF_VAR_repository_id=$_REPOSITORY_NAME",
    "TF_VAR_service_account_id=$_SERVICEACCOUNT_ID",
    "TF_VAR_gke_machine_type=$_GKE_MACHINE_TYPE"] 


    cloudbuild_cd_environment = [ 
    "PROJECT_ID=$PROJECT_ID",
    "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
    "DNS_NAME=$_DNS_NAME",
    "PR_URL=$(body.pull_request._links.html.href)",
    "TARGET_PR_REPO_BRANCH=$(body.pull_request.base.label)",
    "PR_TYPE=$(body.action)",
    "MERGE_STATUS=$(body.pull_request.merged)",
    "MERGE_COMMIT=$(body.pull_request.merge_commit_sha)",
    "ORIGIN=$_ORIGIN",
    "SUBDIRS=$_SUBDIRS",
    "SDKS=$_SDKS",
    "BEAM_CONCURRENCY=$_BEAM_CONCURRENCY",
    "PR_COMMIT=$_PR_COMMIT",
    "CD_SCRIPT_PATH=beam/playground/infrastructure/cloudbuild/playground_cd_examples.sh",
    "FORCE_CD=false",
    ]

    cloudbuild_cd_environment_manual = [ 
    "PROJECT_ID=$PROJECT_ID",
    "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
    "DNS_NAME=$_DNS_NAME",
    "PR_URL=URL",
    "TARGET_PR_REPO_BRANCH=apache:master",
    "PR_TYPE=closed",
    "MERGE_STATUS=true",
    "MERGE_COMMIT=$_MERGE_COMMIT",
    "ORIGIN=$_ORIGIN",
    "SUBDIRS=$_SUBDIRS",
    "SDKS=$_SDKS",
    "BEAM_CONCURRENCY=$_BEAM_CONCURRENCY",
    "PR_COMMIT=$_PR_COMMIT",
    "CD_SCRIPT_PATH=beam/playground/infrastructure/cloudbuild/playground_cd_examples.sh",
    "FORCE_CD=true",
    "PRIVATE_BUCKET=${google_storage_bucket.playground_cloudbuild_private.url}"
    ]

    cloudbuild_ci_environment = [ 
    "PR_BRANCH=$(body.pull_request.head.ref)",
    "PR_URL=$(body.pull_request._links.html.href)",
    "PR_TYPE=$(body.action)",
    "PR_COMMIT=$(body.pull_request.head.sha)",
    "PR_NUMBER=$(body.number)",
    "CI_SCRIPT_PATH=beam/playground/infrastructure/cloudbuild/playground_ci_examples.sh",
    "PRIVATE_BUCKET=${google_storage_bucket.playground_cloudbuild_private.url}",
    "PUBLIC_BUCKET=${google_storage_bucket.playground_cloudbuild_public.url}",
    "PUBLIC_LOG=CI_PR$$(body.number)_$$(body.pull_request.head.sha)_$${BUILD_ID}.txt",
    "PAT_SECRET=github_pat_playground_deployment",
    "FORK_REPO=$(body.pull_request.head.repo.full_name)",
    "BASE_REF=$(body.pull_request.base.ref)"
    ]
    }