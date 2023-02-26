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

resource "random_string" "postfix" {
  length  = 8
  upper   = false
  special = false
}

locals {
  common_resource_name = "${var.resource_name_prefix}-${random_string.postfix.result}"
}

// Provision minimally necessary environment to provision a Google Kubernetes engine.
module "setup" {
  source = "./modules/01-setup"

  project                            = var.project
  kubernetes_node_service_account_id = local.common_resource_name
  region                             = var.region
}

// Provision Google Cloud Virtual Provide Cloud (VPC) network and related resources.
module "network" {
  source = "./modules/02-network"

  kubernetes_node_service_account_email = module.setup.kubernetes_node_service_account_email
  network_base_name                     = local.common_resource_name
  project                               = var.project
  region                                = var.region
  subnetwork_cidr_range                 = var.subnetwork_cidr_range
  required_services = module.setup.required_services
}

// Provision Google Kubernetes Engine cluster.
module "cluster" {
  source = "./modules/03-cluster"

  cluster_name = local.common_resource_name
  kubernetes_node_service_account_email = module.setup.kubernetes_node_service_account_email
  network = module.network.network
  project = var.project
  region = var.region
  subnetwork = module.network.subnetwork
  required_services = module.setup.required_services
}