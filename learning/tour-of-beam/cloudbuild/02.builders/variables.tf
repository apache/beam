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

# Trigger service account variables
variable "tob_deploy_sa" {
    default = "tob-deploy"
    description = "Service account name to be created and used by cloud build deployer"
}

variable "tob_update_sa" {
    default = "tob-update"
    description = "Service account name to be created and used by cloud build updater"
}

variable "tob_ci_sa" {
    default = "tob-ci"
    description = "Service account name to be created and used by cloud build CI"
}

variable "tob_cd_sa" {
    default = "tob-cd"
    description = "Service account name to be created and used by cloud build CD"
}

# Playground variables
variable "project_id" {
    description = "GCP project id where resources will be created"
}

variable "playground_dns_name" {
  description = "The DNS A-record name for Playground website"
  default = "fqdn.playground.zone"
}

variable "pg_region" {
    description = "Existing Beam Playground's region (e.g: us-west1)"
}

variable "pg_gke_zone" {
    description = "Existing Beam Playground GKE cluster's zone (e.g. us-west1-b)"
}

variable "pg_gke_name" {
    description = "Existing Beam Playground GKE cluster's name"
}

variable "pg_datastore_namespace" {
    description = "Existing Beam Playground's datastore namespace"
}

variable "environment_name" {
    description = "Environment name for Tour of Beam backend (e.g. prod, staging). To support multi-environment on same GCP project"
}

variable "tob_learning_root" {
    description = "Existing Tour of Beam learning material root"
    default = "../learning-content/"
}

variable "state_bucket" {
    description = "Existing GCS bucket's name to store Terraform state"
}

# Trigger variables
variable "trigger_source_repo" {
    default = "https://github.com/beamplayground/deploy-workaround"
}

variable "trigger_source_branch" {
  description = "Source branch used for github trigger, not used but reqired due to cloudbuild limitation"
  default = "main"
}

variable "webhook_trigger_secret_id" {
  description = "The name of the secret for webhook config cloud build trigger (CI/CD)"
  default = "playground-cicd-webhook"
}

variable "gh_pat_secret_id" {
  description = "The name of the secret for GitHub Personal Access Token. Required for cloud build trigger (CI/CD)"
  default = "playground-github-pat-ci"
}

variable "tob_deploy_trigger_name" {
  description = "The name of the trigger to run CI checks"
  default = "TourOfBeam-Deploy"
}

variable "tob_ci_trigger_name" {
  description = "The name of the trigger to run CI checks"
  default = "TourOfBeam-CI"
}

variable "tob_cd_trigger_name" {
  description = "The name of the trigger to run CD checks"
    default = "TourOfBeam-CD"
}

variable "cloudbuild_machine_type" {
  description = "Machine type used for cloudbuild runtime"
  default = "E2_HIGHCPU_32"
}

variable "tob_cloudbuild_private_bucket" {
  description = "The Google Cloud Platform GCS bucket name for Tour of Beam Cloudbuild Private logs"
}

variable "tob_cloudbuild_public_bucket" {
  description = "The Google Cloud Platform GCS bucket name for Tour of Beam Cloudbuild Public logs"
}

variable "terraform_source_repo" {
  description = "Repo used to fetch terraform code"
  default = "https://github.com/apache/beam"
}

variable "terraform_source_branch" {
  description = "Branch used to fetch terraform code"
  default = "master"
}