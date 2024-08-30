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
resource "google_service_account" "tourofbeam_deploy_sa" {
  account_id   = var.tourofbeam_deploy_sa
  description  = "The service account to be used by cloud build to deploy Tour of Beam backend"
}

resource "google_service_account" "tourofbeam_ci_sa" {
  account_id   = var.tourofbeam_ci_sa
  description  = "The service account to be used by cloud build to run CI checks for Tour of Beam backend"
}

resource "google_service_account" "tourofbeam_cd_sa" {
  account_id   = var.tourofbeam_cd_sa
  description  = "The service account to be used by cloud build to run CD checks for Tour of Beam backend"
}

# Assign IAM roles to cloud build service account
resource "google_project_iam_member" "tourofbeam_backend_deployer_roles" {
  for_each = toset([
    "roles/datastore.indexAdmin",
    "roles/datastore.user",
    "roles/iam.serviceAccountCreator",
    "roles/iam.serviceAccountUser",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
    "roles/firebase.admin",
    "roles/container.clusterViewer",
    "roles/logging.logWriter",
    "roles/cloudfunctions.admin"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tourofbeam_deploy_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "tourofbeam_ci_sa_roles" {
  for_each = toset([
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService",
    "roles/storage.objectAdmin"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tourofbeam_ci_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "tourofbeam_cd_sa_roles" {
  for_each = toset([
    "roles/datastore.user",
    "roles/storage.objectAdmin",
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService"

  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tourofbeam_cd_sa.email}"
  project = var.project_id
}

