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

variable "examples_bucket_name" {
  description = "Name of Bucket to Store Playground Examples"
  default     = "playground-examples"
}

variable "examples_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "examples_storage_class" {
  description = "Examples Bucket Storage Class"
  default     = "STANDARD"
}

variable "terraform_bucket_name" {
  description = "Name of Bucket to Store Terraform States"
  default     = "playground_terraform"
}

variable "terraform_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "terraform_storage_class" {
  description = "Terrafomr Bucket Storage Class"
  default     = "STANDARD"
}
