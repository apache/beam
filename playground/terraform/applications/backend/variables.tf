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

variable "cache_address" {
  description = "IP Address for Redis service"
  default     = ""
}

variable "environment" {
  description = "prod,dev,beta"
  default     = "dev"
}

variable "network_name" {
  description = "VPC Name"
  default     = "playground-vpc"
}

variable "subnetwork_name" {
  description = "Vpc Name"
  default     = "playground-vpc"
}

variable "docker_image_tag" {
  description = "Docker Image Tag To Be Deployed"
  default     = ""
}

variable "docker_image_name" {
  default     = "beam_playground-backend"
  description = "Base prefix for docker images"
}

variable "docker_registry_address" {
  description = "Docker registry address"
}


variable "base_service_name" {
  default = "backend"
}

variable "cache_type" {
  description = "remote or local. Set remote to use Redis and local to use in-memory application level cache"
  default     = "remote"
}


# SCIO variables

variable "scio_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

# Go variables

variable "go_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

# Python variables


variable "python_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

# Java variables

variable "java_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}

# Router variables

variable "router_volume_size" {
  description = "Size of the in memory file system to be used by the application, in GB"
  type        = number
  default     = 1
}