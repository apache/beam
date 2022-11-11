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

output "cloudbuild_service_account_id" {
  value = google_service_account.cloudbuild_service_account_id
}

output "How_to_connect_github_repo_to_cloudbuild" {
  value = <<EOF

Navigate to https://console.cloud.google.com/cloud-build/triggers/connect?project=${var.project}
to connect Cloud Build to your GitHub repository.

More details: https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github
(Note: skip where it asks you to create a trigger, because trigger will be created further using IaC)
EOF
}