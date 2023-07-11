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
# KIND, either express or implied.  See the License for the```
# specific language governing permissions and limitations
# under the License.

# This creates cloud build trigger for deploying Tour of Beam backend
resource "google_cloudbuild_trigger" "tourofbeam_deployment_trigger" {
  name     = var.tourofbeam_deploy_trigger_name

  description = "Trigger used to deploy Tour of Beam infrastructure and backend"

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

    logs_bucket = "gs://${var.tourofbeam_cb_private_bucket}"

    step {
      id = "Deploy Tour of Beam"
      script = file("../scripts/tob_deploy_infra_backend.sh")
      name = "ubuntu"
      env = local.cloudbuild_init_environment
    }
  }
  
  substitutions = {
    _PG_REGION              = var.pg_region
    _PG_GKE_ZONE            = var.pg_gke_zone
    _PG_GKE_NAME            = var.pg_gke_name
    _PG_DATASTORE_NAMESPACE = var.pg_datastore_namespace
    _DNS_NAME               = var.playground_dns_name
    _WEB_APP_ID             = var.web_app_id
    _STATE_BUCKET           = var.tourofbeam_deployment_state_bucket
    _ENVIRONMENT_NAME       = var.environment_name
    _TOB_LEARNING_ROOT      = var.tourofbeam_learning_root
    _BRANCH_NAME            = var.terraform_source_branch
    _REPO_NAME              = var.terraform_source_repo
    }

  service_account = data.google_service_account.tourofbeam_deployer.id
}

resource "google_cloudbuild_trigger" "tourofbeam_ci_trigger" {
  name    = var.tourofbeam_ci_trigger_name
  project = var.project_id

  description = "Validate changed examples and set commit status messages in GitHub"

  service_account = data.google_service_account.tourofbeam_ci_runner.id

  webhook_config {
    secret = data.google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.tourofbeam_cb_private_bucket}" 
    step {
      id     = "Run CI"
      script = file("../../../../playground/infrastructure/cloudbuild/cloudbuild_playground_ci_examples.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_ci_environment
      secret_env = ["PAT"]
    }
    available_secrets {
      secret_manager {
        env          = "PAT"
        version_name = "${data.google_secret_manager_secret_version.secret_gh_pat_cloudbuild_data.secret}/versions/latest"
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
      _PUBLIC_BUCKET: "${var.tourofbeam_cb_public_bucket}"
      _PUBLIC_LOG_URL: "https://storage.googleapis.com/$${_PUBLIC_BUCKET}/$${_PUBLIC_LOG}"
    }
  }

  substitutions = {
    _BEAM_VERSION             = "2.47.0"
  }

}

resource "google_cloudbuild_trigger" "tourofbeam_cd_trigger" {
  name    = var.tourofbeam_cd_trigger_name
  project = var.project_id

  description = "Automatically update examples for an existing Tour of Beam environment"

  service_account = data.google_service_account.tourofbeam_cd_runner.id

  webhook_config {
    secret = data.google_secret_manager_secret_version.secret_webhook_cloudbuild_trigger_cicd_data.id
  }

  build {
    timeout = "7200s"
    options {
      machine_type = var.cloudbuild_machine_type
      logging      = "GCS_ONLY"
    }
    logs_bucket = "gs://${var.tourofbeam_cb_private_bucket}"
    step {
      id     = "Run Learning Materials CD"
      script = file("../scripts/tob_lm_cd.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_cd_environment_manual
    }
    step {
      id     = "Run Example CD"
      script = file("../../../../playground/infrastructure/cloudbuild/cloudbuild_playground_cd_examples.sh")
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
    _DATASTORE_NAMESPACE   = var.pg_datastore_namespace
    _ORIGIN                = "TB_EXAMPLES"
    _TOB_LEARNING_ROOT     = var.tourofbeam_learning_root
    _SDKS                  = "java python go"
    _SUBDIRS               = "./learning/tour-of-beam/learning-content"
    _BEAM_CONCURRENCY      = "4"
  }

}

resource "google_cloudbuild_trigger" "tourofbeam_cd_manual_trigger" {
  name    = "${var.tourofbeam_cd_trigger_name}-manual"
  project = var.project_id

  description = "Manually update examples for an existing Tour of Beam environment"

  service_account = data.google_service_account.tourofbeam_cd_runner.id

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
    logs_bucket = "gs://${var.tourofbeam_cb_private_bucket}" 

    step {
      id     = "Run Learning Materials CD"
      script = file("../scripts/tob_lm_cd.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_cd_environment_manual
    }
    step {
      id     = "Run Example CD"
      script = file("../../../../playground/infrastructure/cloudbuild/cloudbuild_playground_cd_examples.sh")
      name   = "ubuntu"
      env    = local.cloudbuild_cd_environment_manual
    }
  }

  substitutions = {
    _DNS_NAME            = var.playground_dns_name
    _DATASTORE_NAMESPACE = var.pg_datastore_namespace
    _MERGE_COMMIT        = "master"
    _ORIGIN              = "TB_EXAMPLES"
    _TOB_LEARNING_ROOT   = var.tourofbeam_learning_root
    _SDKS                = "java python go"
    _SUBDIRS             = "./learning/tour-of-beam/learning-content"
    _BEAM_CONCURRENCY    = "4"
  }

}

