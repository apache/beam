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
    description = "Google Project ID to use for deployment"
    
}
variable "region" {
    description = "Google Region to use for deployment"
}
variable "zone" {
    description = "Google Zone to use for deployment"
}
variable "min_main_node_count" {
    description = "Minimal node count for GKE"
    default = "1"
}
variable "max_main_node_count" {
    description = "Maximal node count for GKE"
    default = "2"
}
variable "max_main_replicas" {
    description = "Maximal replicas for Action Runners"
    default = "2"
  
}
variable "min_main_replicas" {
    description = "Minimal replicas for Action Runners"
    default = "1"
  
}
variable machine_type {
    description = "Machine type to use for runner Node Pool"
    default = "e2-standard-2"
}
variable "environment" {
    description = "name of environment"
    default = ""
}
variable "ingress_domain" {
    description = "Domain to use for ingress"
    default = ""
}
variable "state_bucket" {
    description = "State bucket to use for terraform state"
    default = ""
}
variable "organization" {
    description = "Github Organization to use for runners"
}
variable "repository" {
    description = "Respository to attach the runners to"
}
variable "github_app_id_secret_name" {
    description = "Secret Name for Github App ID"
}
variable "github_app_install_id_secret_name" {
    description = "Secret Name for Github App Installation ID"
}
variable "github_private_key_secret_name" {
    description = "Secret Name for Github App Private Key"
}
variable "deploy_webhook" {
    description = "Enable Github Webhook deployment. use this if the Github App has permissions to create webhooks"
    default = "false"
}
variable "runner_group" {
  description = "value for the runner group label"
  default = ""
}
variable "webhook_scaling" {
    description = "Enable scaling of runners based on webhook events"
    default = "false"
  
}
