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

# need enable api
#  https://console.developers.google.com/apis/api/iam.googleapis.com/overview?project=750903720791
resource "google_service_account" "account" {
  project            = "${var.project_id}"
  account_id   = "${var.service_account}"
  display_name = "${var.service_account}"
}


output "playground_service_accoun_id" {
  value = "${google_service_account.account.account_id}"
}


resource "google_project_iam_binding" "account_iam_binding" {
  for_each = toset([
  "roles/dataflow.admin",
  "roles/compute.storageAdmin",
  "roles/iam.serviceAccountTokenCreator"
  ])
  role     = each.value
  project = "${var.project_id}"
  members = [
  "serviceAccount:${google_service_account.account.email}"
  ]
}





