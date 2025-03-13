// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Provision a custom service account for the node pool.
resource "google_service_account" "node_pool" {
  depends_on = [google_project_service.required]
  account_id = local.resource_name
}

// Bind minimally permissive IAM roles to the node pool service account.
// See https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#permissions
resource "google_project_iam_member" "node_pool" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
    "roles/autoscaling.metricsWriter"
  ])
  member  = "serviceAccount:${google_service_account.node_pool.email}"
  project = var.project
  role    = each.key
}
