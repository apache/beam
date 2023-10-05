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

variable "artifact_registry_id" {
  type        = string
  description = "The ID of the artifact registry repository"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "dataflow_worker_service_account_id" {
  type        = string
  description = "The Dataflow Worker Service Account ID"
}

variable "network_name_base" {
  type        = string
  description = "The name of the Google Cloud Platform (GCP) name basis from which we name network related resources"
}

variable "gradle_project" {
  type        = string
  description = "The gradle project to base Dataflow template build"
}

variable "storage_bucket_name" {
  type        = string
  description = "The name of the Storage Bucket reserved for infra pipelines"
}

variable "template_image_prefix" {
  type        = string
  description = "The artifact registry image prefix of the Dataflow template"
}
