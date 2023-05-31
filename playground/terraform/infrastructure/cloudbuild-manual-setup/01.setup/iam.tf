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

resource "google_service_account" "pg_cloudbuild_deploy_sa" {
  account_id   = var.playground_deploy_sa == "" ? "pg-deploy" : var.playground_deploy_sa
  description  = "The service account to be used by cloud build to deploy Playground"
}

resource "google_service_account" "pg_cloudbuild_update_sa" {
  account_id   = var.playground_update_sa == "" ? "pg-update" : var.playground_update_sa
  description  = "The service account to be used by cloud build to update Playground"
}

resource "google_service_account" "pg_cloudbuild_ci_runner_sa" {
  account_id   = var.playground_ci_sa == "" ? "pg-ci" : var.playground_ci_sa
  description  = "The service account to be used by cloud build to run CI scripts and checks"
}

resource "google_service_account" "pg_cloudbuild_cd_runner_sa" {
  account_id   = var.playground_cd_sa == "" ? "pg-cd" : var.playground_cd_sa
  description  = "The service account to be used by cloud build to run CD scripts and checks"
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
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountUser",
    "roles/datastore.indexAdmin",
    "roles/storage.admin",
    "roles/dns.admin",
    "roles/logging.logWriter",
    "roles/cloudfunctions.developer"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.pg_cloudbuild_deploy_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "playground_helm_updater_roles" {
  for_each = toset([
    "roles/artifactregistry.admin",
    "roles/compute.admin",
    "roles/container.admin",
    "roles/storage.admin",
    "roles/iam.serviceAccountUser",
    "roles/iam.roleAdmin",
    "roles/appengine.appAdmin",
    "roles/datastore.indexAdmin",
    "roles/datastore.user",
    "roles/redis.admin",
    "roles/dns.admin",
    "roles/logging.logWriter",
    "roles/cloudfunctions.admin"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.pg_cloudbuild_update_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "playground_ci_sa_roles" {
  for_each = toset([
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.pg_cloudbuild_ci_runner_sa.email}"
  project = var.project_id
}

resource "google_project_iam_member" "playground_cd_sa_roles" {
  for_each = toset([
    "roles/datastore.user",
    "roles/secretmanager.secretAccessor",
    "roles/storage.insightsCollectorService"

  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.pg_cloudbuild_cd_runner_sa.email}"
  project = var.project_id
}

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_project_iam_member" "cloudbuild_sa_role" {
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
  project = var.project_id
  depends_on = [ google_project_service.required_services ]
}
