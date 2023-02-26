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

variable "kubernetes_node_service_account_email" {
  type        = string
  description = "The Google Cloud Platform Service Account email to be used by the node VMs created by GKE"
}

variable "network" {
  description = "The Google Cloud Platform Virtual Cloud network within which we provision the kubernetes node"
}

variable "subnetwork" {
  description = "The Google Cloud Platform Virtual Cloud subnetwork within which we provision the kubernetes node"
}

variable "cluster_name" {
  type = string
  description = "The name of the Google Kubernetes engine cluster."
}

variable "required_services" {
  description = "A hack to wait for turning on Google Project API services prior to applying this module."
}
