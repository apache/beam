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

variable "resource_name_prefix" {
  type        = string
  description = "The basis to name all provisioned resources i.e. service account, network, cluster, etc."
}

variable "subnetwork_cidr_range" {
  type        = string
  description = "The address range for this subnet, in CIDR notation. Use a standard private VPC network address range: for example, 10.128.0.0/20"
}

variable "bastion_compute_machine_type" {
  type        = string
  description = "The machine type of the Bastion host. See gcloud compute machine-types list for available types, for example e2-standard-2"
}
