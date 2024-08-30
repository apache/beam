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
   "BRANCH_NAME=$_BRANCH_NAME",
   "REPO_NAME=$_REPO_NAME" ,
   "PG_REGION=$_PG_REGION",
   "DNS_NAME=$_DNS_NAME",
   "WEB_APP_ID=$_WEB_APP_ID",
   "PG_GKE_ZONE=$_PG_GKE_ZONE",
   "PG_GKE_NAME=$_PG_GKE_NAME",
   "PROJECT_ID=$PROJECT_ID",
   "STATE_BUCKET=$_STATE_BUCKET",

   # Learning material 
   "DATASTORE_PROJECT_ID=$PROJECT_ID",
   "DATASTORE_NAMESPACE=$_PG_DATASTORE_NAMESPACE",
   "TOB_LEARNING_ROOT=$_TOB_LEARNING_ROOT",

   # Terraform variables
   "TF_VAR_environment=$_ENVIRONMENT_NAME",
   "TF_VAR_region=$_PG_REGION",
   "TF_VAR_project_id=$PROJECT_ID",
   "TF_VAR_datastore_namespace=$_PG_DATASTORE_NAMESPACE",
   ]

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
   # Learning material 
   "DATASTORE_PROJECT_ID=$PROJECT_ID",
   "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
   "TOB_LEARNING_ROOT=$_TOB_LEARNING_ROOT",
    ]

   cloudbuild_cd_environment_manual = [ 
   "PROJECT_ID=$PROJECT_ID",
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
   # Learning material 
   "DATASTORE_PROJECT_ID=$PROJECT_ID",
   "DATASTORE_NAMESPACE=$_DATASTORE_NAMESPACE",
   "TOB_LEARNING_ROOT=$_TOB_LEARNING_ROOT",
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
