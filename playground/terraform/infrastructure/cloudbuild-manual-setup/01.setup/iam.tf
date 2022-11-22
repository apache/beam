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

resource "google_service_account" "cloudbuild_service_account_id" {
  account_id   = var.cloudbuild_service_account_id
  display_name = var.cloudbuild_service_account_id
  description  = "The service account cloud build will use to deploy Playground"
}

// Provision IAM roles to the IaC service account required to build and provision resources
resource "google_project_iam_member" "cloud_build_roles" {
  for_each = toset([
    "roles/appengine.appAdmin",
    "roles/appengine.appCreator",
    "roles/artifactregistry.admin",
    "roles/redis.admin",
    "roles/compute.admin",
    "roles/iam.serviceAccountCreator",
    "roles/container.admin",
    "roles/servicemanagement.quotaAdmin",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountUser",
    "roles/datastore.indexAdmin",
    "roles/storage.admin",
    "roles/logging.logWriter"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloudbuild_service_account_id.email}"
  project = var.project_id
}