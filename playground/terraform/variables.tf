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

variable "env" {
  description = "prod,dev,beta"
}

variable "region" {
  description = "Infrastructure Region"
}

variable "zone" {
  description = "Infrastructure Zone"
}

variable "state_bucket" {}
# Infrastructure variables

#GKE

variable "gke_machine_type" {
  description = "Node pool machine types"
  default     = "e2-standard-8"
}

variable "gke_name" {
  description = "Name of GKE cluster"
  default = "playground-backend"
}

variable "min_count" {
  description = "Min cluster node count"
  default     = 2
}

variable "max_count" {
  description = "Max cluster node count"
  default     = 6
}

variable "service_account" {
  description = "Service account id"
  default     = "playground-deploy@apache-beam-testing.iam.gserviceaccount.com"
}
# Artifact Registry

variable "repository_id" {
  description = "ID of Artifact Registry"
  default     = "playground-repository"
}

#IAM
variable "service_account_id" {
  description = "Service account ID"
  default     = "beam-playground"
}

#Network
variable "ip_address_name" {
  description = "Static IP address name"
  default     = "pg-static-ip"
}

variable "subnetwork_name" {
  description = "Name of VPC to be created"
  default     = "playground-vpc-sub"
}

#AppEngine Flag
variable "skip_appengine_deploy" {
  description = "AppEngine enabled"
  type        = bool
  default     = false
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

#VPC
variable "network_name" {
  description = "Name of VPC to be created"
  default     = "playground-network"
}

# Applications

variable "create_default_service" {
  description = "Whether or not to create a default app engine service"
  type        = bool
  default = true
}

variable "docker_image_tag" {
  description = "Docker Image Tag To Be Deployed"
  default     = ""
}

variable "docker_image_name" {
  default     = "beam_playground"
  description = "Base prefix for docker images"
}
