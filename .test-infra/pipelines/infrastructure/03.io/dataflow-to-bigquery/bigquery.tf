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

// Provision BigQuery dataset sink to store Dataflow API Job data
resource "google_bigquery_dataset" "sink" {
  dataset_id  = replace(var.workflow_resource_name_base, "-", "_")
  description = "Stores Dataflow API Jobs data"
}

// Provision IAM roles to write to BigQuery sink
resource "google_bigquery_dataset_iam_member" "dataflow_worker_roles" {
  dataset_id = google_bigquery_dataset.sink.dataset_id
  member     = "serviceAccount:${data.google_service_account.dataflow_worker.email}"
  role       = "roles/bigquery.dataEditor"
}
