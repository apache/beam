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

variable "storage_bucket_prefix" {
  type        = string
  description = "Storage Bucket Prefix that stores the JAR files"
}

variable "storage_bucket_location" {
  type        = string
  description = "The GCS location"
}

variable "looker_jar_endpoint" {
  type        = string
  description = "The endpoint from which to download Looker jars"
  default     = "https://apidownload.looker.com/download"
}

variable "looker_version" {
  type        = string
  description = "Looker <major version>.<minor version> - e.g. 22.14"
}

variable "license_secret_name" {
  type        = string
  description = "The Google Secret Manager name that stores the Looker License"
}

variable "license_email" {
  type        = string
  description = "The email associated with the Looker license"
}

variable "prometheus_version" {
  type        = string
  description = "The version of the JMX prometheus javaagent."
}
