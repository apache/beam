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

data "google_service_account" "cloudbuild_sa" {
  account_id = var.cloudbuild_service_account_id
}

resource "google_cloudbuild_trigger" "playground_infrastructure" {
  name     = var.infra_trigger_name
  project  = var.project_id

  description = "Builds the base image and then runs cloud build config file to deploy Playground infrastructure"

  github {
    owner = var.github_repository_owner
    name  = var.github_repository_name
    push {
      branch = var.github_repository_branch
    }
  }

  substitutions = {
    _PLAYGROUND_REGION : var.playground_region
    _PLAYGROUND_ZONE : var.playground_zone
    _ENVIRONMENT_NAME : var.playground_environment_name
    _DNS_NAME : var.playground_dns_name
    _NETWORK_NAME : var.playground_network_name
    _GKE_NAME : var.playground_gke_name
    _STATE_BUCKET : var.state_bucket
  }

  filename = "playground/infrastructure/cloudbuild/cloudbuild_pg_infra.yaml"

  service_account = data.google_service_account.cloudbuild_sa.id
}

resource "google_cloudbuild_trigger" "playground_to_gke" {
  name     = var.gke_trigger_name
  project  = var.project_id

  description = "Builds the base image and then runs cloud build config file to deploy Playground to GKE"

  github {
    owner = var.github_repository_owner
    name  = var.github_repository_name
    push {
      branch = var.github_repository_branch
    }
  }

  substitutions = {
    _PLAYGROUND_REGION : var.playground_region
    _PLAYGROUND_ZONE : var.playground_zone
    _ENVIRONMENT_NAME : var.playground_environment_name
    _DNS_NAME : var.playground_dns_name
    _NETWORK_NAME : var.playground_network_name
    _GKE_NAME : var.playground_gke_name
    _STATE_BUCKET : var.state_bucket
    _TAG : var.image_tag
    _DOCKER_REPOSITORY_ROOT : var.docker_repository_root
    _SDK_TAG : var.sdk_tag
  }

  filename = "playground/infrastructure/cloudbuild/cloudbuild_pg_to_gke.yaml"

  service_account = data.google_service_account.cloudbuild_sa.id
}