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

resource "google_cloudbuild_trigger" "playground_infrastructure" {
  name        = var.infra_trigger_id
  location    = var.region
  description = "Builds the base image and then runs cloud build config file to deploy Playground infrastructure"
  github {
    owner = var.github_repository_owner
    name  = var.github_repository_name
    push {
      branch = var.github_repository_branch
    }
  }
  // Disabled because we only want to run it manually
  disabled = true

  service_account = var.cloudbuild_service_account_id
  filename = "playground/infrastructure/cloudbuild/cloudbuild_pg_infra.yaml"

  }

resource "google_cloudbuild_trigger" "playground_to_gke" {
  name        = var.gke_trigger_id
  location    = var.region
  description = "Builds the base image and then runs cloud build config file to deploy Playground to GKE"
  github {
    owner = var.github_repository_owner
    name  = var.github_repository_name
    push {
      branch = var.github_repository_branch
    }
  }
  // Disabled because we only want to run it manually
  disabled = true

  service_account = var.cloudbuild_service_account_id
  filename = "playground/infrastructure/cloudbuild/cloudbuild_pg_to_gke.yaml"

}