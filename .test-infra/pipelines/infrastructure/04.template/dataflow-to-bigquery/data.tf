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

// Query the Artifact Registry repository.
data "google_artifact_registry_repository" "default" {
  location      = var.region
  repository_id = var.artifact_registry_id
}

// Query the Dataflow Worker service account.
data "google_service_account" "dataflow_worker" {
  account_id = var.dataflow_worker_service_account_id
}

// Query the GCP project.
data "google_project" "default" {
  project_id = var.project
}

// Query the GCP Network.
data "google_compute_network" "default" {
  name = var.network_name_base
}

// Query the GCP Subnetwork.
data "google_compute_subnetwork" "default" {
  region = var.region
  name   = var.network_name_base
}

// Query the Storage bucket.
data "google_storage_bucket" "default" {
  name = var.storage_bucket_name
}

locals {
  template_file_gcs_path = "gs://${data.google_storage_bucket.default.name}/templates/dataflow-to-bigquery.json"
}
