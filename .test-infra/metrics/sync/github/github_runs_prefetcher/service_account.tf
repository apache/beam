# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_service_account" "service_account" {
  account_id   = "gh-workflow-prefetcher-invoker"
  display_name = "Github Workflow Prefetcher Invoker Account"
}

resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.prefetcher.project
  region         = google_cloudfunctions_function.prefetcher.region
  cloud_function = google_cloudfunctions_function.prefetcher.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${google_service_account.service_account.email}"
}