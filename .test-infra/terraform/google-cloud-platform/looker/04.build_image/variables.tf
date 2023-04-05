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

variable "resource_name_prefix" {
  type        = string
  description = "The basis to name all provisioned resources i.e. service account, network, cluster, etc."
}

variable "looker_jars_glob" {
  type        = string
  description = "The Google Cloud Storage glob path of the looker jars. Example: gs://looker-jars-12345/23.4/*.jars"
}

variable "artifact_registry_url" {
  type        = string
  description = "The Artifact Registry URL to which to store the built image. Expected format: <location>-docker.pkg.dev/<project>/looker"
}
