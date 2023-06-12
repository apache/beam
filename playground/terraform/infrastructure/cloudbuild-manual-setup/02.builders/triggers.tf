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
  name    = var.pg_infra_trigger_name
  project = var.project_id

  description = "Deploy GCP infrastructure required for Playground GKE cluster deployment"

  source_to_build {
    uri       = var.trigger_source_repo
    ref       = "refs/heads/${var.trigger_source_branch}"
    repo_type = "GITHUB"
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.cloudbuild_bucket_private}" 

    step {
      id     = "run_gradle"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_infra.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_init_environment
    }

    substitutions = {
      _NETWORK_NAME       = "$${_ENVIRONMENT_NAME}-playground-network"
      _SUBNETWORK_NAME    = "$${_ENVIRONMENT_NAME}-playground-subnetwork"
      _GKE_NAME           = "$${_ENVIRONMENT_NAME}-playground-gke"
      _IPADDRESS_NAME     = "$${_ENVIRONMENT_NAME}-playground-ipaddress"
      _REDIS_NAME         = "$${_ENVIRONMENT_NAME}-playground-redis"
      _SERVICEACCOUNT_ID  = "$${_ENVIRONMENT_NAME}-playground-sa"
      _DOCKER_REPO_NAME   = "$${_ENVIRONMENT_NAME}-playground-docker"
      _REDIS_TIER             = var.redis_tier
    }

  }

  substitutions = {
    _REPO_NAME              = var.terraform_source_repo
    _BRANCH_NAME            = var.terraform_source_branch
    _SKIP_APPENGINE_DEPLOY  = var.skip_appengine_deploy
    _GKE_MACHINE_TYPE       = var.gke_machine_type
    _ENVIRONMENT_NAME       = var.playground_environment_name
    _INIT_MIN_COUNT         = var.init_min_count
    _MAX_COUNT              = var.max_count
    _MIN_COUNT              = var.min_count
    _PLAYGROUND_REGION      = var.playground_region
    _PLAYGROUND_ZONE        = var.playground_zone
    _STATE_BUCKET           = var.state_bucket
  }

  service_account = data.google_service_account.playground_infra_deploy_sa.id
}

resource "google_cloudbuild_trigger" "playground_to_gke" {
  name    = var.pg_gke_trigger_name
  project = var.project_id

  description = "Deploy (or update) Playground GKE cluster"

  source_to_build {
    uri       = var.trigger_source_repo
    ref       = "refs/heads/${var.trigger_source_branch}"
    repo_type = "GITHUB"
  }
  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.cloudbuild_bucket_private}" 
    step {
      id     = "run_gradle"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_deploy.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_deploy_environment
    }
    substitutions = {
      _NETWORK_NAME         = "$${_ENVIRONMENT_NAME}-playground-network"
      _SUBNETWORK_NAME      = "$${_ENVIRONMENT_NAME}-playground-subnetwork"
      _GKE_NAME             = "$${_ENVIRONMENT_NAME}-playground-gke"
      _IPADDRESS_NAME       = "$${_ENVIRONMENT_NAME}-playground-ipaddress"
      _REDIS_NAME           = "$${_ENVIRONMENT_NAME}-playground-redis"
      _SERVICEACCOUNT_ID    = "$${_ENVIRONMENT_NAME}-playground-sa"
      _DOCKER_REPO_NAME     = "$${_ENVIRONMENT_NAME}-playground-docker"
      _DATASTORE_NAMESPACE  = "playground-$${_ENVIRONMENT_NAME}"
      _REDIS_TIER           = var.redis_tier
    }

  }
  substitutions = {
    _REPO_NAME              = var.terraform_source_repo
    _BRANCH_NAME            = var.terraform_source_branch
    _SKIP_APPENGINE_DEPLOY  = var.skip_appengine_deploy
    _DNS_NAME               = var.playground_dns_name
    _GKE_MACHINE_TYPE       = var.gke_machine_type
    _ENVIRONMENT_NAME       = var.playground_environment_name
    _MAX_COUNT              = var.max_count
    _MIN_COUNT              = var.min_count
    _PLAYGROUND_REGION      = var.playground_region
    _PLAYGROUND_ZONE        = var.playground_zone
    _SDK_TAG                = var.sdk_tag
    _STATE_BUCKET           = var.state_bucket
    _CONTAINER_TAG          = var.image_tag
  }

  service_account = data.google_service_account.playground_update_sa.id
}

resource "google_cloudbuild_trigger" "playground_ci" {
  name    = var.pg_ci_trigger_name
  project = var.project_id

  description = "Validate changed examples and set commit status messages in GitHub"

  service_account = data.google_service_account.playground_ci_sa.id

  webhook_config {
    secret = google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.cloudbuild_bucket_private}" 
    step {
      id     = "Run CI"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_ci_examples.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_ci_environment
      secret_env = ["PAT"]
    }
    available_secrets {
      secret_manager {
        env          = "PAT"
        version_name = "${google_secret_manager_secret_version.secret_gh_pat_cloudbuild_data.secret}/versions/latest"
      }
    }
    substitutions = {
      _PR_BRANCH            = "$(body.pull_request.head.ref)"
      _PR_URL               = "$(body.pull_request._links.html.href)"
      _PR_TYPE              = "$(body.action)"
      _PR_COMMIT            = "$(body.pull_request.head.sha)"
      _PR_NUMBER            = "$(body.number)"
      _FORK_REPO            = "$(body.pull_request.head.repo.full_name)"
      _BASE_REF             = "$(body.pull_request.base.ref)"
      _PUBLIC_LOG_LOCAL: "/tmp/$${_PUBLIC_LOG}"
      _PUBLIC_LOG: "CI_PR$${_PR_NUMBER}_$${_PR_COMMIT}_$${BUILD_ID}.txt"
      _PUBLIC_BUCKET: "${var.cloudbuild_bucket_public}"
      _PUBLIC_LOG_URL: "https://storage.googleapis.com/$${_PUBLIC_BUCKET}/$${_PUBLIC_LOG}"
    }
  }

  substitutions = {
    _BEAM_VERSION             = "2.44.0"
  }

}

resource "google_cloudbuild_trigger" "playground_cd" {
  name    = var.pg_cd_trigger_name
  project = var.project_id

  description = "Automatically update examples for an existing Playground environment"

  service_account = data.google_service_account.playground_cd_sa.id

  webhook_config {
    secret = google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.cloudbuild_bucket_private}" 
    step {
      id     = "Run CD"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_cd_examples.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_cd_environment
    }
    substitutions = {
      _PR_URL                = "$(body.pull_request._links.html.href)"
      _TARGET_PR_REPO_BRANCH = "$(body.pull_request.base.label)"
      _PR_TYPE               = "$(body.action)"
      _MERGE_STATUS          = "$(body.pull_request.merged)"
      _MERGE_COMMIT          = "$(body.pull_request.merge_commit_sha)"
    }
  }

  substitutions = {
    _DNS_NAME              = var.playground_dns_name
    _DATASTORE_NAMESPACE   = "playground-env"
    _ORIGIN                = "PG_EXAMPLES"
    _SDKS                  = "java python go"
    _SUBDIRS               = "./learning/katas ./examples ./sdks"
    _BEAM_CONCURRENCY      = "4"
  }

}

resource "google_cloudbuild_trigger" "playground_cd_manual" {
  name    = "${var.pg_cd_trigger_name}-manual"
  project = var.project_id

  description = "Manually update examples for an existing Playground environment"

  service_account = data.google_service_account.playground_cd_sa.id

  source_to_build {
    uri       = var.trigger_source_repo
    ref       = "refs/heads/${var.trigger_source_branch}"
    repo_type = "GITHUB"
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.cloudbuild_bucket_private}" 
    step {
      id     = "Run CD"
      script = file("../../../../infrastructure/cloudbuild/cloudbuild_playground_cd_examples.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_cd_environment_manual
    }
  }

  substitutions = {
    _DNS_NAME            = "fqdn.playground.zone"
    _DATASTORE_NAMESPACE = "playground-env"
    _MERGE_COMMIT        = "master"
    _ORIGIN              = "PG_EXAMPLES"
    _SDKS                = "java python go"
    _SUBDIRS             = "./learning/katas ./examples ./sdks"
    _BEAM_CONCURRENCY    = "4"
  }

}

