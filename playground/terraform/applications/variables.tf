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

variable "environment" {
  description = "Environment name, e.g. prod,dev,beta"
  default     = "dev"
}

variable "network_name" {
  description = "Vpc Name"
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

variable "backend_service_name" {
  default = "backend"
}

variable "docker_image_name" {
  default     = "beam_playground"
  description = "Base prefix for docker images"
}

variable "cache_type" {
  description = "remote or local. Set remote to use Redis and local to use in-memory application level cache"
  default     = "remote"
}

variable "location" {
  description = "Location of App"
  default     = "us-central"
}

variable "app_domain" {
  default = "lm.r.appspot.com"
}

# Frontend variables

variable "frontend_service_name" {
  default = "frontend"
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


variable "create_default_service" {
  description = "Whether or not to create a default app engine service"
  type        = bool
  default = true
}

variable "state_bucket" {}
variable "state_prefix" {}
