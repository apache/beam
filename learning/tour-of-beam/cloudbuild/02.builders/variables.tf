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


variable "project_id" {
    description = "GCP project id where resources will be created"
}

# Trigger service account variables
variable "tourofbeam_deploy_sa" {
    description = "Service account name to be created and used by cloud build deployer"
}

variable "tourofbeam_ci_sa" {
    description = "Service account name to be created and used by cloud build CI"
}

variable "tourofbeam_cd_sa" {
    description = "Service account name to be created and used by cloud build CD"
}

# Playground variables
variable "environment_name" {
    description = "Environment name for Tour of Beam backend (e.g. prod, staging). To support multi-environment on same GCP project"
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

variable "tourofbeam_deployment_state_bucket" {
    description = "Existing GCS bucket's to store Terraform state for Tour of Beam deployment"
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

variable "tourofbeam_deploy_trigger_name" {
  description = "The name of the trigger to deploy Tour of Beam"
  default = "TourOfBeam-Deploy"
}

variable "tourofbeam_ci_trigger_name" {
  description = "The name of the trigger to run CI checks"
  default = "TourOfBeam-CI"
}

variable "tourofbeam_cd_trigger_name" {
  description = "The name of the trigger to run CD checks"
    default = "TourOfBeam-CD"
}

variable "cloudbuild_machine_type" {
  description = "Machine type used for cloudbuild runtime"
  default = "E2_HIGHCPU_32"
}

variable "tourofbeam_cb_private_bucket" {
  description = "The Google Cloud Platform GCS bucket name for Tour of Beam Cloudbuild Private logs"
}

variable "tourofbeam_cb_public_bucket" {
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

variable "tourofbeam_learning_root" {
    description = "Existing Tour of Beam learning material root"
    default = "../learning-content/"
}

variable "web_app_id" {
    description = "Tour of Beam web app id"
    default = "tourofbeam-web-app"
}
