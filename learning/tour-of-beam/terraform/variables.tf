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

variable "cloudfunctions_bucket" {
  description = "The bucket name that will store functions' source code"
}

variable "project_id" {
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "service_account_id" {
  description = "The ID of the service account responsible for running Google Cloud functions"
}

variable "region" {
  description = "The region of the Google Cloud project within which resources are provisioned"
}

variable "gcloud_init_account" {
  description = "User Account ID logged in with gcloud init command (e.g. username@domain.com)"
}

variable "pg_router_host" {
  description = "Hostname:port of Playground GKE cluster's router grpc workload"
}

variable "environment" {
  description = "The name of the environment for deployment. Will create directory where terraform config files will be stored"
}
