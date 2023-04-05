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
  name     = var.pg_infra_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger to deploy Playground infrastructure"

  source_to_build {
    uri       = "https://github.com/beamplayground/deploy-workaround"
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path = ""
    uri       = "https://github.com/beamplayground/deploy-workaround"
    revision       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  substitutions = {
    _APP_ENGINE_FLAG : var.appengine_flag
    _GKE_MACHINE_TYPE : var.gke_machine_type
    _ENVIRONMENT_NAME : var.playground_environment_name
    _GKE_NAME : var.playground_gke_name
    _IPADDRESS_NAME : var.ipaddress_name
    _MAX_COUNT : var.max_count
    _MIN_COUNT : var.min_count
    _NETWORK_NAME : var.playground_network_name
    _PLAYGROUND_REGION : var.playground_region
    _PLAYGROUND_ZONE : var.playground_zone
    _REDIS_NAME: var.redis_name
    _REDIS_TIER: var.redis_tier
    _REPOSITORY_ID: var.docker_repository_root
    _SERVICE_ACCOUNT_ID: var.playground_service_account
    _STATE_BUCKET: var.state_bucket
    _SUBNETWORK_NAME: var.playground_subnetwork_name
  }

  service_account = data.google_service_account.playground_infra_deploy_sa.id
}

resource "google_cloudbuild_trigger" "playground_to_gke" {
  name     = var.pg_gke_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger to deploy Playground to GKE"

  source_to_build {
    uri       = "https://github.com/beamplayground/deploy-workaround"
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path = ""
    uri       = "https://github.com/beamplayground/deploy-workaround"
    revision       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  substitutions = {
    _APP_ENGINE_FLAG : var.appengine_flag
    _DATASTORE_NAMESPACE: var.datastore_namespace
    _DNS_NAME: var.playground_dns_name
    _GKE_MACHINE_TYPE : var.gke_machine_type
    _ENVIRONMENT_NAME : var.playground_environment_name
    _GKE_NAME : var.playground_gke_name
    _IP_ADDRESS_NAME : var.ipaddress_name
    _MAX_COUNT : var.max_count
    _MIN_COUNT : var.min_count
    _NETWORK_NAME : var.playground_network_name
    _PLAYGROUND_REGION : var.playground_region
    _PLAYGROUND_ZONE : var.playground_zone
    _REDIS_NAME: var.redis_name
    _REDIS_TIER: var.redis_tier
    _REPOSITORY_ID: var.docker_repository_root
    _SDK_TAG: var.sdk_tag
    _SERVICE_ACCOUNT_ID: var.playground_service_account
    _STATE_BUCKET: var.state_bucket
    _SUBNETWORK_NAME: var.playground_subnetwork_name
    _TAG_NAME: var.image_tag
  }

  service_account = data.google_service_account.playground_infra_deploy_sa.id
}

resource "google_cloudbuild_trigger" "playground_helm_update" {
  name     = var.pg_helm_upd_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger for Playground Helm update"

  source_to_build {
    uri       = "https://github.com/beamplayground/deploy-workaround"
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path = ""
    uri       = "https://github.com/beamplayground/deploy-workaround"
    revision  = "refs/heads/master"
    repo_type = "GITHUB"
  }

  substitutions = {
    _APP_ENGINE_FLAG : var.appengine_flag
    _DATASTORE_NAMESPACE: var.datastore_namespace
    _DNS_NAME: var.playground_dns_name
    _GKE_MACHINE_TYPE : var.gke_machine_type
    _ENVIRONMENT_NAME : var.playground_environment_name
    _GKE_NAME : var.playground_gke_name
    _IP_ADDRESS_NAME : var.ipaddress_name
    _MAX_COUNT : var.max_count
    _MIN_COUNT : var.min_count
    _NETWORK_NAME : var.playground_network_name
    _PLAYGROUND_REGION : var.playground_region
    _PLAYGROUND_ZONE : var.playground_zone
    _REDIS_NAME: var.redis_name
    _REDIS_TIER: var.redis_tier
    _REPOSITORY_ID: var.docker_repository_root
    _BEAM_VERSION: var.sdk_tag
    _SERVICE_ACCOUNT_ID: var.playground_service_account
    _STATE_BUCKET: var.state_bucket
    _SUBNETWORK_NAME: var.playground_subnetwork_name
    _TAG_NAME: var.image_tag
  }

  service_account = data.google_service_account.playground_helm_upd_sa.id
}

resource "google_cloudbuild_trigger" "playground_ci" {
  name     = var.pg_ci_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger for Playground CI checks"

  service_account = data.google_service_account.playground_cicd_sa.id

  webhook_config {
    secret = google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    step {
      name = "bash"
      args = ["-c", "echo 'Hello, world!'"]
    }
  }

  substitutions = {
    _BEAM_VERSION : var.sdk_tag
  }

}

resource "google_cloudbuild_trigger" "playground_cd" {
  name     = var.pg_ci_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger for Playground CD checks"

  service_account = data.google_service_account.playground_cicd_sa.id

  webhook_config {
    secret = google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    step {
      name = "bash"
      args = ["-c", "echo 'Hello, world!'"]
    }
  }

  substitutions = {
    _DNS_NAME : var.playground_dns_name
    _DATASTORE_NAMESPACE: var.datastore_namespace
  }
}