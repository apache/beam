#
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
#

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "region" {
  description = "The GCP region within which we provision resources"
  default     = "us-central1"
}

#IAM

variable "service_account_id" {
  description = "Service account ID"
  default     = "beam-playground"
}

#GCS

variable "examples_bucket_name" {
  description = "Name of Bucket to Store Playground Examples"
  default     = "playground-examples"
}

variable "examples_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "examples_storage_class" {
  description = "Examples Bucket Storage Class"
  default     = "STANDARD"
}

variable "terraform_bucket_name" {
  description = "Name of Bucket to Store Terraform States"
  default     = "playground_terraform"
}

variable "terraform_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "terraform_storage_class" {
  description = "Terraform Bucket Storage Class"
  default     = "STANDARD"
}

# Artifact Registry

variable "repository_id" {
  description = "ID of Artifact Registry"
  default     = "playground-repository"
}

variable "repository_location" {
  description = "Location of Artifact Registry"
  default     = "us-central1"
}

variable "service_account" {
  description = "Service account id"
  default     = "service-account-playground"
}

#Redis

variable "redis_version" {
  description = "The GCP Project ID where Playground Applications will be created"
  default     = "REDIS_6_X"
}

variable "terraform_state_bucket_name" {
  description = "Bucket name for terraform state"
  default     = "beam_playground_terraform"
}

variable "redis_region" {
  description = "Region of Redis"
  default     = "us-central1"
}

variable "redis_name" {
  description = "Name of Redis"
  default     = "playground-backend-cache"
}

variable "redis_tier" {
  description = "Tier of Redis"
  default     = "STANDARD_HA"
}

variable "redis_replica_count" {
  description = "Redis's replica count"
  default     = 1
}

variable "redis_memory_size_gb" {
  description = "Size of Redis memory"
  default     = 5
}

variable "read_replicas_mode" {
  description = "Read replica mode. Can only be specified when trying to create the instance."
  default     = "READ_REPLICAS_ENABLED"
}

#VPC

variable "vpc_name" {
  description = "Name of VPC to be created"
  default     = "playground-vpc"
}

variable "create_subnets" {
  description = "Auto Create Subnets Inside VPC"
  default     = true
}

variable "mtu" {
  description = "MTU Inside VPC"
  default     = 1460
}

# GKE


variable "gke_machine_type" {
  description = "Node pool machine types"
  default     = "e2-standard-4"
}

variable "gke_node_count" {
  description = "Node pool size"
  default     = 1
}

variable "gke_name" {
  description = "Name of GKE cluster"
  default     = "playground-examples"
}

variable "gke_location" {
  description = "Location of GKE cluster"
  default     = "us-central1-a"
}
