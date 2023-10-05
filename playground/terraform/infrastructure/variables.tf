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

#COMMON

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "environment" {
  description = "Environment name, e.g. prod,dev,beta"
}

variable "region" {
  description = "Infrastructure Region"
}

variable "env" {}

variable "state_bucket" {}

#IAM

variable "service_account_id" {
  description = "Service account ID"
  default     = "beam-playground"
}

variable "service_account" {
  description = "Service account email"
  default     = "service-account-playground"
}

variable "bucket_terraform_state_name" {
  description = "Name of Bucket to Store Terraform States"
  default     = "beam_playground_terraform"
}

variable "bucket_terraform_state_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "bucket_terraform_state_storage_class" {
  description = "TerraformBucket Storage Class"
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

variable "repository_domain" {
  default = "-docker.pkg.dev"
}

#REDIS

variable "redis_version" {
  description = "The GCP Project ID where Playground Applications will be created"
  default     = "REDIS_6_X"
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
  default     = "BASIC"
}

variable "redis_replica_count" {
  description = "Redis's replica count"
  default     = 0
}

variable "redis_memory_size_gb" {
  description = "Size of Redis memory ,  if set 'read replica' it must be from 5GB to 100GB."
  default     = 5
}

variable "read_replicas_mode" {
  description = "Read replica mode. Can only be specified when trying to create the instance."
  default     = "READ_REPLICAS_DISABLED"
}

#NETWORK

variable "network_name" {
  description = "Name of VPC to be created"
  default     = "playground-vpc"
}

variable "subnetwork_name" {
  description = "Name of VPC to be created"
  default     = "playground-vpc-sub"
}

variable "network_region" {
  description = "Region of Redis"
  default     = "us-central1"
}

variable "ip_address_name" {
  description = "Static IP address name"
  default     = "pg-static-ip"
}

# APPENGINE
variable "skip_appengine_deploy" {
  description = "AppEngine enabled"
  type        = bool
  default     = false
}

# GKE

variable "gke_machine_type" {
  description = "Node pool machine types"
  default     = "e2-standard-8"
}

variable "gke_name" {
  description = "Name of GKE cluster"
  default     = "playground-examples"
}

variable "gke_location" {
  description = "Location of GKE cluster"
}

variable "location" {
  description = "Location of GKE cluster"
}
variable "init_min_count" {
  description = "Initial cluster node count"
  default     = 1
}
variable "min_count" {
  description = "Min cluster node count"
  default     = 2
}

variable "max_count" {
  description = "Max cluster node count"
  default     = 6
}
