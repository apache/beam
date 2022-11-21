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
  location = var.region
  project  = var.project_id

  description = "Builds the base image and then runs cloud build config file to deploy Playground infrastructure"

  source_to_build {
    uri       = "https://github.com/apache/beam"
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path      = "playground/infrastructure/cloudbuild/cloudbuild_pg_infra.yaml"
    repo_type = "GITHUB"
  }

  substitutions = {
    _ENVIRONMENT_NAME: var.playground_environment_name
    _DNS_NAME: var.playground_dns_name
    _NETWORK_NAME: var.playground_network_name
    _GKE_NAME: var.playground_gke_name
    _STATE_BUCKET: var.state_bucket
  }

  service_account = data.google_service_account.cloudbuild_sa.id
}

resource "google_cloudbuild_trigger" "playground_to_gke" {
  name     = var.gke_trigger_name
  location = var.region
  project  = var.project_id

  description = "Builds the base image and then runs cloud build config file to deploy Playground to GKE"

  source_to_build {
    uri       = "https://github.com/apache/beam"
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path      = "playground/infrastructure/cloudbuild/cloudbuild_pg_infra.yaml"
    repo_type = "GITHUB"
  }

  substitutions = {
    _ENVIRONMENT_NAME: var.playground_environment_name
    _DNS_NAME: var.playground_dns_name
    _NETWORK_NAME: var.playground_network_name
    _GKE_NAME: var.playground_gke_name
    _STATE_BUCKET: var.state_bucket
    _TAG: var.image_tag
    _DOCKER_REPOSITORY_ROOT: var.docker_repository_root
  }

  service_account = data.google_service_account.cloudbuild_sa.id
}