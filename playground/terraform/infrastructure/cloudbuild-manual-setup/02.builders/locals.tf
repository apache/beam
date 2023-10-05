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
    "TF_VAR_env=$_ENVIRONMENT_NAME",  
    "TF_VAR_project_id=$PROJECT_ID",
    "TF_VAR_network_name=$_NETWORK_NAME",
    "TF_VAR_subnetwork_name=$_SUBNETWORK_NAME",
    "TF_VAR_gke_name=$_GKE_NAME",
    "TF_VAR_region=$_PLAYGROUND_REGION",
    "TF_VAR_zone=$_PLAYGROUND_ZONE",
    "TF_VAR_state_bucket=$_STATE_BUCKET",
    "TF_VAR_redis_name=$_REDIS_NAME",
    "TF_VAR_redis_tier=$_REDIS_TIER",
    "TF_VAR_init_min_count=$_INIT_MIN_COUNT",
    "TF_VAR_min_count=$_MIN_COUNT",
    "TF_VAR_max_count=$_MAX_COUNT",
    "TF_VAR_skip_appengine_deploy=$_SKIP_APPENGINE_DEPLOY",
    "TF_VAR_ip_address_name=$_IPADDRESS_NAME",
    "TF_VAR_repository_id=$_DOCKER_REPO_NAME",
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
    "TF_VAR_env=$_ENVIRONMENT_NAME",  
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
    "TF_VAR_repository_id=$_DOCKER_REPO_NAME",
    "TF_VAR_service_account_id=$_SERVICEACCOUNT_ID",
    "TF_VAR_gke_machine_type=$_GKE_MACHINE_TYPE"] 


    cloudbuild_cd_environment = [ 
    "PROJECT_ID=$PROJECT_ID",
    "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
    "DNS_NAME=$_DNS_NAME",
    "PR_URL=$_PR_URL",
    "TARGET_PR_REPO_BRANCH=$_TARGET_PR_REPO_BRANCH",
    "PR_TYPE=$_PR_TYPE",
    "MERGE_STATUS=$_MERGE_STATUS",
    "MERGE_COMMIT=$_MERGE_COMMIT",
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
    "FORCE_CD=true"
    ]

    cloudbuild_ci_environment = [ 
    "PROJECT_ID=$PROJECT_ID",
    "PR_BRANCH=$_PR_BRANCH",
    "PR_URL=$_PR_URL",
    "PR_TYPE=$_PR_TYPE",
    "PR_COMMIT=$_PR_COMMIT",
    "PR_NUMBER=$_PR_NUMBER",
    "CI_SCRIPT_PATH=beam/playground/infrastructure/cloudbuild/playground_ci_examples.sh",
    "PUBLIC_BUCKET=$_PUBLIC_BUCKET",
    "PUBLIC_LOG=$_PUBLIC_LOG",
    "PUBLIC_LOG_URL=$_PUBLIC_LOG_URL",
    "PUBLIC_LOG_LOCAL=$_PUBLIC_LOG_LOCAL",
    "FORK_REPO=$_FORK_REPO",
    "BASE_REF=$_BASE_REF",
    "BEAM_VERSION=$_BEAM_VERSION"
    ]
    }