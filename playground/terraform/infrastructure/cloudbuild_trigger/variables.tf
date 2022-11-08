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
  type = string
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "region" {
  type = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "cloudbuild_sa_id" {
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
}

variable "github_repository_name" {
  type        = string
  description = "The name of the GitHub repository. For example the repository name for https://github.com/example/foo is 'foo'."
}

variable "github_repository_owner" {
  type        = string
  description = "Owner of the GitHub repository. For example the owner for https://github.com/example/foo is 'example'."
}

variable "github_repository_branch" {
  type        = string
  description = "The GitHub repository branch regex to match cloud build trigger"
}

variable "image_tag" {
  type = string
  description = "The tag to apply to image builds"
}

variable "trigger_id" {
  type = string
  description = "The name of the trigger that will be created"
}