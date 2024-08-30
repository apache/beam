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

// Provision a service account that will be bound to the Dataflow pipeline
resource "google_service_account" "dataflow_worker" {
  depends_on   = [google_project_service.required_services]
  account_id   = var.dataflow_worker_service_account_id
  display_name = var.dataflow_worker_service_account_id
  description  = "The service account bound to the compute engine instance provisioned to run Dataflow Jobs"
}

// Provision IAM roles for the Dataflow runner service account
resource "google_project_iam_member" "dataflow_worker_service_account_roles" {
  depends_on = [google_project_service.required_services]
  for_each   = toset([
    "roles/dataflow.worker",
    "roles/dataflow.viewer"
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataflow_worker.email}"
  project = var.project
}
