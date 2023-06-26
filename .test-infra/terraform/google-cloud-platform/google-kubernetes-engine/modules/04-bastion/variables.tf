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

variable "kubernetes_node_service_account" {
  type = object({
    email = string
  })
  description = "The Google Cloud Platform Service Account bound to the GKE node"
}

variable "network" {
  type = object({
    name = string
  })
  description = "The Google Cloud Platform Virtual Cloud network within which we provision the kubernetes node"
}

variable "subnetwork" {
  type = object({
    name = string
  })
  description = "The Google Cloud Platform Virtual Cloud subnetwork within which we provision the kubernetes node"
}

variable "bastion_compute_machine_type" {
  type        = string
  description = "The machine type of the Bastion host. See gcloud compute machine-types list for available types"
}

variable "router" {
  type = object({
    name = string
  })
  description = "The Google Cloud Platform Virtual Cloud router"
}

variable "nat" {
  type = object({
    name = string
  })
  description = "The Google Cloud Platform Virtual Cloud router NAT"
}
