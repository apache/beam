/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Provision a service account to bind to the Kubernetes cluster node
resource "google_service_account" "kubernetes_node_service_account" {
  account_id   = var.kubernetes_node_service_account_id
  display_name = var.kubernetes_node_service_account_id
  description  = "The service account bound to the Kubernetes node"
}

// Provision IAM roles for the kubernetes node service account
resource "google_project_iam_member" "kubernetes_node_service_account_roles" {
  depends_on = [google_project_service.required_services]

  // provision as toset to make it easier to add new IAM roles in the future
  for_each = toset([
    "roles/container.nodeServiceAccount",
  ])

  role    = each.key
  member  = "serviceAccount:${google_service_account.kubernetes_node_service_account.email}"
  project = var.project

}