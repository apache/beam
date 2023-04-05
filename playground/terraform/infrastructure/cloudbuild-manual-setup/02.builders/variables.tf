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
  type        = string
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region (For example: us-central1) where Cloud Build triggers will be created at"
}

variable "infra_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground infrastructure"
  default     = "playground-infrastructure-trigger"
}

variable "gke_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground to GKE"
  default     = "playground-to-gke-trigger"
}

variable "cloudbuild_service_account_id" {
  type        = string
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
  default     = "playground-cloudbuild-sa"
}

variable "github_repository_name" {
  type        = string
  description = "The name of the GitHub repository. For example the repository name for https://github.com/example/foo is 'foo'."
}

variable "github_repository_owner" {
  type        = string
  description = "The owner of the GitHub repository. For example the owner for https://github.com/example/foo is 'example'."
}

variable "github_repository_branch" {
  type        = string
  description = "The GitHub repository branch regex to match cloud build trigger"
}

variable "playground_environment_name" {
  description = <<EOF
Environment name where to deploy Playground. Located in playground/terraform/environment/{environment_name}. E.g. test, dev, prod.
More details: https://github.com/akvelon/beam/blob/cloudbuild%2Bmanualsetup%2Bplayground/playground/terraform/README.md#prepare-deployment-configuration"
  EOF
}

variable "playground_dns_name" {
  description = <<EOF
The DNS record name for Playground website.
More details: https://github.com/apache/beam/blob/master/playground/terraform/README.md#deploy-playground-infrastructure"
  EOF
}

variable "playground_network_name" {
  description = "The Google Cloud Platform (GCP) VPC Network Name for Playground deployment"
}

variable "playground_gke_name" {
  description = "The Playground GKE Cluster name in Google Cloud Platform (GCP)"
}

variable "state_bucket" {
  description = "The Google Cloud Platform (GCP) GCS bucket name for Beam Playground temp files and Terraform state"
}

variable "image_tag" {
  description = "The docker images tag for Playground containers"
}

variable "docker_repository_root" {
  description = "The name of Google Cloud Platform (GCP) Artifact Registry Repository where Playground images will be saved to"
}

variable "playground_region" {
  description = "The Google Cloud Platform (GCP) region (For example: us-central1) where playground infrastructure will be deployed to"
}

variable "playground_zone" {
  description = "The Google Cloud Platform (GCP) zone (For example: us-central1-b) where playground infrastructure will be deployed to"
}

variable "sdk_tag" {
  description = <<EOF
Apache Beam Golang and Python images SDK tag. (For example: 2.43.0)
See more: https://hub.docker.com/r/apache/beam_python3.7_sdk/tags and https://hub.docker.com/r/apache/beam_go_sdk"
  EOF
}