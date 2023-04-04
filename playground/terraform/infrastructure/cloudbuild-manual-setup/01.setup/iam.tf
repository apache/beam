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

resource "google_service_account" "playground_deploy_sa" {
  account_id   = var.playground_deploy_sa
  display_name = var.playground_deploy_sa
  description  = "The service account cloud build will use to deploy Playground"
}

resource "google_service_account" "playground_helm_upd_sa" {
  account_id   = var.playground_helm_upd_sa
  display_name = var.playground_helm_upd_sa
  description  = "The service account cloud build will use to deploy Playground"
}

resource "google_service_account" "playground_cicd_sa" {
  account_id   = var.playground_cicd_sa
  display_name = var.playground_cicd_sa
  description  = "The service account cloud build will use to run CI/CD scripts for Playground"
}

// Provision IAM roles to the IaC service account required to build and provision resources
resource "google_project_iam_member" "playground_deployer_roles" {
  for_each = toset([
    "roles/appengine.appAdmin",
    "roles/appengine.appCreator",
    "roles/artifactregistry.admin",
    "roles/redis.admin",
    "roles/compute.admin",
    "roles/iam.serviceAccountCreator",
    "roles/container.admin",
    "roles/servicemanagement.quotaAdmin",
    "roles/iam.roleAdmin",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountUser",
    "roles/datastore.indexAdmin",
    "roles/storage.admin",
    "roles/logging.logWriter"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.playground_deploy_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "playground_helm_updater_roles" {
  for_each = toset([
    "roles/artifactregistry.admin",
    "roles/compute.admin",
    "roles/container.admin",
    "roles/logging.logWriter"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.playground_helm_upd_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "playground_cicd_sa_roles" {
  for_each = toset([
    "roles/artifactregistry.reader",
    "roles/storage.admin",
    "roles/logging.logWriter"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.playground_helm_upd_sa.email}"
  project = var.project_id
}