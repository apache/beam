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

# Service account for GCP Cloud Functions
resource "google_service_account" "cloud_function_sa" {
  account_id   = local.cloudfunctions_service_account
  display_name = "Tour of Beam CF Service Account-${var.environment}"
}

# IAM roles for Cloud Functions service account
resource "google_project_iam_member" "terraform_service_account_roles" {
  for_each = toset([
    "roles/cloudfunctions.admin", "roles/storage.objectViewer",
    "roles/iam.serviceAccountUser", "roles/datastore.user",
    "roles/firebaseauth.viewer"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
  project = var.project_id
}
