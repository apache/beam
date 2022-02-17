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

# Common variables

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "environment" {
  description = "prod,dev,beta"
}

variable "region" {
  description = "Infrastructure Region"
  default     = "us-central1"
}

# Infrastructure variables

#GKE

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

variable "service_account" {
  description = "Service account id for example service-account-playground@friendly-tower-340607.iam.gserviceaccount.com"
  default     = "service-account-playground@friendly-tower-340607.iam.gserviceaccount.com"
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

# Artifact Registry

variable "repository_id" {
  description = "ID of Artifact Registry"
  default     = "playground-repository"
}

variable "repository_location" {
  description = "Location of Artifact Registry"
  default     = "us-central1"
}

#Redis

variable "redis_version" {
  description = "The GCP Project ID where Playground Applications will be created"
  default     = "REDIS_6_X"
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
  description = "Size of Redis memory ,  if set 'read replica' it must be from 5GB to 100GB."
  default     = 5
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

# Applications


variable "docker_image_tag" {
  description = "Docker Image Tag To Be Deployed"
  default     = ""
}


variable "docker_image_name" {
  default     = "beam_playground"
  description = "Base prefix for docker images"
}

# Frontend variables

variable "frontend_service_name" {
  default = "frontend"
}

# Backend variables

variable "cache_type" {
  description = "remote or local. Set remote to use Redis and local to use in-memory application level cache"
  default     = "remote"
}
variable "backend_service_name" {
  default = "backend"
}


variable "scio_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

variable "go_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

variable "python_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

variable "java_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

variable "router_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

variable "state_bucket" {}
variable "state_prefix" {}