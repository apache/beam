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
variable "existing_vpc_name" {
    description = "Name of existing VPC to use for deployment"
    default = ""
}
variable "existing_ip_name" {
    description = "Name of existing IP to use for ingress"
    default = ""
}
variable "subnetwork_cidr_range" { 
    description = "CIDR range for subnetwork"
    default = "10.128.0.0/20"
  
}
variable "service_account_id" {
    description = "ID of service account to use for deployment. This can be Name, full Email or Fully Qualified Path"
    default = ""
}
variable "runner_group" {
  description = "value for the runner group label"
  default = ""
}

variable "main_runner" {
    type = object({
      name = string
      machine_type = optional(string, "e2-standard-2")
      min_node_count = optional(number, 1)
      max_node_count = optional(number, 1)
      min_replicas = optional(number, 1)
      max_replicas = optional(number, 1)
      disk_size_gb = optional(number, 100)
      webhook_scaling = optional(bool, false)
      runner_image = optional(string, "summerwind/actions-runner:v2.304.0-ubuntu-20.04-30355f7")
      labels = optional(list(string), ["self-hosted", "ubuntu-20.04","main"])
      enable_selector = optional(bool, false)
      enable_taint = optional(bool, false)
      requests = optional(object({
        cpu = string
        memory = string
        }), { cpu = "500m",
              memory = "500Mi" 
        })
      limits = optional(object({
        cpu = optional(string)
        memory = optional(string)
        }), {
            cpu = "",
            memory = ""
        })
    })
}
variable "additional_runner_pools" {
    type = list(object({
      name = string
      machine_type = optional(string, "e2-standard-2")
      min_node_count = optional(number, 1)
      max_node_count = optional(number, 1)
      min_replicas = optional(number, 1)
      max_replicas = optional(number, 1)
      disk_size_gb = optional(number, 100)
      webhook_scaling = optional(bool, false)
      runner_image = optional(string, "summerwind/actions-runner:v2.304.0-ubuntu-20.04-30355f7")
      labels = optional(list(string), ["self-hosted", "ubuntu-20.04","changeme"])
      enable_selector = optional(bool, true)
      enable_taint = optional(bool, true)
      requests = optional(object({
        cpu = string
        memory = string
        }), { cpu = "500m",
              memory = "500Mi" 
        })
      limits = optional(object({
        cpu = optional(string)
        memory = optional(string)
        }), {
            cpu = "",
            memory = ""
        })
    }))
    default = []
}