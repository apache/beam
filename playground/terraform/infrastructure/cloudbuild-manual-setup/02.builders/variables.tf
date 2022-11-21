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
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "infra_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground infrastructure"
  default     = "Playground-infrastructure-trigger"
}

variable "gke_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground to GKE"
  default     = "Playground-to-gke-trigger"
}

variable "cloudbuild_service_account_id" {
  type        = string
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
  default     = "playground-cloudbuild-sa"
}

variable "playground_environment_name" {
  description = "Environment where to deploy Playground. Located in playground/terraform/environment/{environment_name}"
}

variable "playground_dns_name" {
  description = "DNS record name for Playground website"
}

variable "playground_network_name" {
  description = "GCP VPC Network Name for Playground deployment"
}

variable "playground_gke_name" {
  description = "Playground GKE Cluster name"
}

variable "state_bucket" {
  description = "GCS bucket name for Beam Playground temp files and Terraform state"
}

variable "image_tag" {
  description = "The tag name for docker images of Playground containers"
}

variable "docker_repository_root" {
  description = "The name of GCP Artifact Registry Repository where Playground images will be saved to"
}