/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

variable "namespace" {
  type        = string
  description = "The Kubernetes namespace"
}

variable "cache_host" {
  type        = string
  description = "The address of the cache i.e. host:port"
}

variable "log_level" {
  type        = string
  description = "Assigns the log level output"
}

variable "echo_service" {
  type = object({
    name  = string
    port  = number
    image = string
  })

  description = "Echo service configuration"
}

variable "quota_service" {
  type = object({
    name  = string
    port  = number
    image = string
  })

  description = "Quota service configuration"
}

variable "refresher_service_image" {
  type        = string
  description = "The refresh service image name"
}

variable "image_repository" {
  type        = string
  description = "The repository from which to pull images i.e. us-central1-docker.pkg.dev/project/infra-pipelines"
}

variable "service_account_name" {
  type        = string
  description = "The Kubernetes service account with permissions to create Jobs"
}