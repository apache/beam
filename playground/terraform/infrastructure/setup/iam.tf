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

resource "google_service_account" "playground_service_account" {
  account_id   = var.service_account_id
  display_name = var.service_account_id
}

resource "google_service_account" "playground_service_account_cf" {
  account_id   = "${google_service_account.playground_service_account.account_id}-cf"
  display_name = "${google_service_account.playground_service_account.account_id}-cf"
}

resource "google_project_iam_member" "terraform_service_account_roles" {
  for_each = toset([
     "roles/container.nodeServiceAccount", "roles/datastore.viewer", "roles/artifactregistry.reader", "roles/logging.logWriter", "roles/monitoring.metricWriter", "roles/stackdriver.resourceMetadata.writer", "roles/bigquery.readSessionUser", "roles/bigquery.dataViewer", "roles/bigquery.jobUser", 
  ])
  role    = each.value
  member  = "serviceAccount:${google_service_account.playground_service_account.email}"
  project = var.project_id
}

resource "google_project_iam_member" "cloudfunction" {
   for_each = toset([
     "roles/storage.objectViewer","roles/cloudfunctions.invoker","roles/datastore.user",
   ])
   role    = each.key
   member  = "serviceAccount:${google_service_account.playground_service_account_cf.email}"
   project = var.project_id
}
