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
    uri       = var.trigger_source_repo
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  build {
    timeout = "7200s"
    options {
      logging = "CLOUD_LOGGING_ONLY"
    }
    step {
      id = "run_gradle"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_infra.sh")
      name = "ubuntu"
      env = local.cloudbuild_init_environment
        }
  }

  substitutions = {
    _REPO_NAME : var.terraform_source_repo
    _BRANCH_NAME: var.terraform_source_branch
    _SKIP_APPENGINE_DEPLOY : var.skip_appengine_deploy
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
    _REPOSITORY_NAME: var.docker_repository_name
    _SERVICEACCOUNT_ID: var.playground_service_account
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
    uri       = var.trigger_source_repo
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }
  build {
    timeout = "7200s"
    options {
      logging = "CLOUD_LOGGING_ONLY"
    }
    step {
      id = "run_gradle"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_deploy.sh")
      name = "ubuntu"
      env = local.cloudbuild_deploy_environment
        }
  }
  substitutions = {
    _REPO_NAME : var.terraform_source_repo
    _BRANCH_NAME: var.terraform_source_branch
    _SKIP_APPENGINE_DEPLOY : var.skip_appengine_deploy
    _DATASTORE_NAMESPACE: var.datastore_namespace
    _DNS_NAME: var.playground_dns_name
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
    _SDK_TAG: var.sdk_tag
    _SERVICEACCOUNT_ID: var.playground_service_account
    _REPOSITORY_NAME: var.docker_repository_name
    _STATE_BUCKET: var.state_bucket
    _SUBNETWORK_NAME: var.playground_subnetwork_name
    _CONTAINER_TAG: var.image_tag
  }

  service_account = data.google_service_account.playground_infra_deploy_sa.id
}

resource "google_cloudbuild_trigger" "playground_helm_update" {
  name     = var.pg_helm_upd_trigger_name
  project  = var.project_id

  description = "Creates cloud build manual trigger for Playground Helm update"

  source_to_build {
    uri       = var.trigger_source_repo
    ref       = "refs/heads/master"
    repo_type = "GITHUB"
  }

  git_file_source {
    path      = ""
    uri       = var.trigger_source_repo
    revision  = "refs/heads/master"
    repo_type = "GITHUB"
  }

  substitutions = {
    _REPO_NAME : var.terraform_source_repo
    _BRANCH_NAME: var.terraform_source_branch
    _SKIP_APPENGINE_DEPLOY : var.skip_appengine_deploy
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
    _REPOSITORY_NAME: var.docker_repository_name
    _SDK_TAG: var.sdk_tag
    _SERVICE_ACCOUNT_ID: var.playground_service_account
    _STATE_BUCKET: var.state_bucket
    _SUBNETWORK_NAME: var.playground_subnetwork_name
    _TAG_NAME: var.image_tag
    _CONTAINER_TAG: var.image_tag

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
    _PRIVATE_LOGS_BUCKET: var.private_logs_bucket
    _PAT_SECRET_NAME: var.gh_pat_secret
  }

}

resource "google_cloudbuild_trigger" "playground_cd" {
  name     = var.pg_cd_trigger_name
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
