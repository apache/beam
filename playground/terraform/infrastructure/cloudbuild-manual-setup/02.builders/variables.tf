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

variable "pg_infra_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground infrastructure"
  default     = "playground-infrastructure-trigger"
}

variable "pg_gke_trigger_name" {
  type        = string
  description = "The name of the trigger that will deploy Playground to GKE"
  default     = "playground-to-gke-trigger"
}

variable "pg_helm_upd_trigger_name" {
  type        = string
  description = "The name of the trigger that run Helm update"
  default     = "playground-helm-update-trigger"
}

variable "cloudbuild_service_account_id" {
  type        = string
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
  default     = "playground-cloudbuild-sa"
}

variable "playground_environment_name" {
  description = "To define environment name which will have it is own configuration of Playground"
}

variable "playground_dns_name" {
  description = "The DNS record name for Playground website"
}

variable "playground_network_name" {
  description = "The Google Cloud Platform (GCP) VPC Network Name for Playground deployment"
}

variable "playground_subnetwork_name" {
  description = "The GCP VPC Subnetwork Name for Playground deployment"
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

variable "appengine_flag" {
  description = "Boolean. If AppEngine and Datastore need to be installed. Put 'false' if AppEngine and Datastore already installed"
}

variable "gke_machine_type" {
  description = "Machine type for GKE Nodes"
  default = "e2-standard-8"
}

variable "ipaddress_name" {
  description = "Defines Static IP Address name for Playground"
  default = "playground-static-ip"
}

variable "max_count" {
  description = "Max node count for GKE cluster"
  default = 4
}

variable "min_count" {
  description = "Min node count for GKE cluster"
  default = 2
}

variable "redis_name" {
  description = "The name of redis instance"
}

variable "redis_tier" {
  description = "The tier of the redis instance (BASIC, STANDARD)"
}

variable "playground_service_account" {
  description = "Service account name for Playground GKE"
}

variable "datastore_namespace" {
  description = "The name of Datastore namespace"
}