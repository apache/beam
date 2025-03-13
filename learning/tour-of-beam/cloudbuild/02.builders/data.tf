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

# Takes data of service account created for cloud build trigger
 data "google_service_account" "tourofbeam_deployer" {
   account_id   = var.tourofbeam_deploy_sa
 }
 
 data "google_service_account" "tourofbeam_ci_runner" {
    account_id   = var.tourofbeam_ci_sa
  }

  data "google_service_account" "tourofbeam_cd_runner" {
    account_id   = var.tourofbeam_cd_sa
  }

# Takes data of secretst created for cloud build trigger
  data "google_secret_manager_secret_version" "secret_webhook_cloudbuild_trigger_cicd_data" {
  secret      = var.webhook_trigger_secret_id
  version     = "latest"
}

  data "google_secret_manager_secret_version" "secret_gh_pat_cloudbuild_data" {
  secret      = var.gh_pat_secret_id
  version     = "latest"
}
 