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

resource "google_artifact_registry_repository" "default" {
  description   = "Stores artifacts related to github.com/apache/beam/.test-infra/pipelines"
  format        = "DOCKER"
  repository_id = var.artifact_registry_id
  location      = var.region
}

resource "google_artifact_registry_repository_iam_member" "dataflow_worker" {
  member     = "serviceAccount:${google_service_account.dataflow_worker.email}"
  repository = google_artifact_registry_repository.default.id
  role       = "roles/artifactregistry.reader"
}