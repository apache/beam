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
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "pg_infra_trigger_name" {
  description = "The name of the trigger that will deploy Playground infrastructure"
  default     = "Initialize-Playground-environment"
}

variable "pg_gke_trigger_name" {
  description = "The name of the trigger that will deploy Playground to GKE"
  default     = "Deploy-Update-Playground-environment"
}
variable "pg_ci_trigger_name" {
  description = "The name of the trigger to run CI checks"
  default = "Playground-CI-stable"
}

variable "pg_cd_trigger_name" {
  description = "The name of the trigger to run CD checks"
  default = "Playground-CD-stable"
}

variable "playground_deploy_sa" {
  description = "The ID of the cloud build service account responsible for deploying the Playground"
}

variable "playground_update_sa" {
  description = "The ID of the cloud build service account responsible for updating the Helm"
}

variable "playground_ci_sa" {
  description = "The ID of the cloud build service account responsible for running CI checks and scripts"
}
variable "playground_cd_sa" {
  description = "The ID of the cloud build service account responsible for running CD checks and scripts"
}


variable "playground_environment_name" {
  description = "An environment name which will have it is own configuration of Playground"
  default = "env"
}

variable "playground_dns_name" {
  description = "The DNS A-record name for Playground website"
  default = "fqdn.playground.zone"
}

variable "state_bucket" {
  description = "The Google Cloud Platform GCS bucket name for Playground Terraform state file"
  default = "playground-tfstate-project-env"
}

variable "cloudbuild_bucket_private" {
  description = "The Google Cloud Platform GCS bucket name for Playground Cloudbuild private logs"
}

variable "cloudbuild_bucket_public" {
  description = "The Google Cloud Platform GCS bucket name for Playground Cloudbuild public logs"
}

variable "image_tag" {
  description = "The docker images tag for Playground images"
  default = "env-1.0"
}

variable "playground_region" {
  description = "The Google Cloud Platform (GCP) region (For example: us-central1) where playground infrastructure will be deployed to"
  default = "us-central1"
}

variable "playground_zone" {
  description = "The Google Cloud Platform (GCP) zone (For example: us-central1-b) where playground infrastructure will be deployed to"
  default = "us-central1-a"
}

variable "sdk_tag" {
  description = <<EOF
Apache Beam Golang and Python images SDK tag. (For example current latest: 2.44.0)
See more: https://hub.docker.com/r/apache/beam_python3.7_sdk/tags and https://hub.docker.com/r/apache/beam_go_sdk"
  EOF
  default = "2.44.0"
}

variable "skip_appengine_deploy" {
  description = "Boolean. If AppEngine and Datastore need to be installed. Put 'true' if AppEngine and Datastore already installed"
  default = "false"
}

variable "gke_machine_type" {
  description = "Machine type for GKE Nodes. Default: e2-standard-8"
  default = "e2-standard-8"
}
variable "init_min_count" {
  description = "value for initial node count for GKE cluster. Default: 1"
  default = 1
}
variable "max_count" {
  description = "Max node count for GKE cluster. Default: 4"
  default = 4
}

variable "min_count" {
  description = "Min node count for GKE cluster. Default: 2"
  default = 2
}

variable "redis_tier" {
  description = "The tier of the GCP redis instance (BASIC, STANDARD)"
  default = "BASIC"
}

variable "webhook_trigger_secret_id" {
  description = "The name of the secret for webhook config cloud build trigger (CI/CD)"
}

variable "gh_pat_secret_id" {
  description = "The name of the secret for GitHub Personal Access Token. Required for cloud build trigger (CI/CD)"
}

variable "data_for_github_pat_secret" {
  description = "The GitHub generated Personal Access Token value"
}
#What i understand this is mandatoy but not actually used. Should we document it 
variable "trigger_source_repo" {
  description = "Source repo used for github trigger, not used but reqired due to cloudbuild limitation"
  default = "https://github.com/beamplayground/deploy-workaround"
}
variable "trigger_source_branch" {
  description = "Source branch used for github trigger, not used but reqired due to cloudbuild limitation"
  default = "main"
}

variable "terraform_source_repo" {
  description = "Repo used to fetch terraform code"
  default = "https://github.com/apache/beam"
}

variable "terraform_source_branch" {
  description = "Branch used to fetch terraform code"
  default = "master"
}
variable "cloudbuild_machine_type" {
  description = "Machine type used for cloudbuild runtime"
  default = "E2_HIGHCPU_32"
}
variable "data_for_cicd_webhook_secret" {
  description = "secret string that was set when creating the webhook in Github"  
}