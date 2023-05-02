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

resource "google_service_account" "tourofbeam_deployer" {
  account_id   = var.tourofbeam_deployer_sa_name
  description  = "The service account to be used by cloud build to deploy Tour of Beam backend"
}

resource "google_project_iam_member" "tourofbeam_backend_deployer" {
  for_each = toset([
    "roles/datastore.indexAdmin",
    "roles/iam.serviceAccountCreator",
    "roles/iam.securityAdmin",
    "roles/iam.serviceAccountUser",
    "roles/serviceusage.serviceUsageAdmin",
    "roles/storage.admin",
    "roles/container.clusterViewer",
    "roles/logging.logWriter"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.tourofbeam_deployer.email}"
  project = var.project_id
}