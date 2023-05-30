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

// Generate random string to name Storage bucket
resource "random_string" "default" {
  length  = 8
  special = false
  upper   = false
  lower   = true
  numeric = true
}

// Provision Storage Bucket for use by Dataflow Worker as temporary storage
resource "google_storage_bucket" "default" {
  location = var.region
  name     = "infra-pipelines-${random_string.default.result}"
  labels   = {
    purpose = "infra-pipelines"
  }
  uniform_bucket_level_access = true
}

// Enable Dataflow Worker Service Account to manage objects in temporary storage
resource "google_storage_bucket_iam_member" "default" {
  bucket = google_storage_bucket.default.id
  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
  role   = "roles/storage.objectAdmin"
}
