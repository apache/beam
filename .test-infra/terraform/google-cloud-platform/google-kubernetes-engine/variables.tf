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

variable "project" {
  type        = string
  description = "The Google Cloud Platform (GCP) project within which resources are provisioned"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "cluster_name_prefix" {
  type        = string
  description = "The prefix to assign the provisioned Google Kubernetes Engine (GKE) cluster; a random string is appended to this value"
}

variable "cluster_name_override" {
  type = string
  description = "Use this to override naming and omit the postfix. Leave empty to use prefix-suffix format"
  default = ""
}

variable "network" {
  type        = string
  description = "The Google Cloud Virtual Private Cloud (VPC) network name"
}

variable "router" {
  type        = string
  description = "The name of the Google Compute Router resource associated with the VPC Network"
}

variable "router_nat" {
  type        = string
  description = "The name of the NAT associated with the Google Compute Router"
}

variable "subnetwork" {
  type        = string
  description = "The Google Cloud Virtual Private Cloud (VPC) subnetwork name"
}

variable "service_account_id" {
  type        = string
  description = "The ID of the service account bound to the Google Kubernetes Engine (GKE) cluster"
}
