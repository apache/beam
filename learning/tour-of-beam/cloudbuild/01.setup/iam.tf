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

# Create cloud build service account
resource "google_service_account" "tob_deploy_sa" {
  account_id   = var.tob_deploy_sa == "" ? "tob-deploy" : var.tob_deploy_sa
  description  = "The service account to be used by cloud build to deploy Tour of Beam backend"
}

resource "google_service_account" "tob_update_sa" {
  account_id   = var.tb_update_sa == "" ? "tob-update" : var.tb_update_sa
  description  = "The service account to be used by cloud build to update Tour of Beam backend"
}

resource "google_service_account" "tob_ci_sa" {
  account_id   = var.tb_ci_sa == "" ? "tob-ci" : var.tb_ci_sa
  description  = "The service account to be used by cloud build to run CI checks for Tour of Beam backend"
}

resource "google_service_account" "tob_cd_sa" {
  account_id   = var.tb_cd_sa == "" ? "tob-cd" : var.tb_cd_sa
  description  = "The service account to be used by cloud build to run CD checks for Tour of Beam backend"
}

# Assign IAM roles to cloud build service account
resource "google_project_iam_member" "tourofbeam_backend_deployer_roles" {
  for_each = toset([
    "roles/datastore.indexAdmin",
    "roles/iam.serviceAccountCreator",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountUser",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
    "roles/container.clusterViewer",
    "roles/logging.logWriter",
    "roles/cloudfunctions.admin"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tob_deploy_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "tourofbeam_backend_updater_roles" {
  for_each = toset([
    "roles/datastore.indexAdmin",
    "roles/iam.serviceAccountCreator",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountUser",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
    "roles/container.clusterViewer",
    "roles/logging.logWriter",
    "roles/cloudfunctions.admin"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tob_update_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "tob_ci_sa_roles" {
  for_each = toset([
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tob_ci_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "tob_cd_sa_roles" {
  for_each = toset([
    "roles/datastore.user",
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService"

  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tob_cd_sa.email}"
  project = var.project_id
}

