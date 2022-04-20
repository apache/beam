#
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
#

#resource "google_service_account" "terraform_service_account" {
#  account_id   = "terraform"
#  display_name = "terraform"
#}
#
#resource "google_project_iam_member" "terraform_service_account_roles" {
#  for_each = toset([
#    // TODO: add the required roles to provision resources (not OWNER :-)!)
#  ])
#  role    = each.key
#  member  = "serviceAccount:${google_service_account.terraform_service_account.email}"
#  project = var.project_id
#}
#
#resource "google_service_account_iam_binding" "terraform_service_account_token_permissions" {
#  service_account_id = google_service_account.terraform_service_account.id
#  members = [
#    "user:${var.developer_account_email}" // TODO: add variable
#  ]
#  role    = "roles/iam.serviceAccountTokenCreator"
#}

#resource "google_service_account_iam_binding" "application_service_account_binding" {
#  members            = [
#    "serviceAccount:${google_service_account.terraform_service_account.email}"
#  ]
#  role               = "roles/iam.serviceAccountUser"
#  service_account_id = google_service_account.playground_service_account.id
#}

resource "google_service_account" "playground_service_account" {
  account_id   = var.service_account_id
  display_name = var.service_account_id
}

resource "google_project_iam_member" "terraform_service_account_roles" {
  for_each = toset([
    "roles/container.serviceAgent",
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.playground_service_account.email}"
  project = var.project_id
}