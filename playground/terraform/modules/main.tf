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

provider "google" {
  region = "us-central"
}
provider "google-beta" {
  region = "us-central"
}


module "vpc" {
  source     = "./vpc"
  project_id = var.project_id
}

module "buckets" {
  source                = "./buckets"
  project_id            = var.project_id
  terraform_bucket_name = var.terraform_bucket_name
  examples_bucket_name  = var.examples_bucket_name
}

module "artifact_registry" {
  source        = "./artifact_registry"
  project_id    = var.project_id
  repository_id = var.repository_id
  depends_on    = [module.buckets]
}

module "memorystore" {
  source                      = "./memorystore"
  project_id                  = var.project_id
  terraform_state_bucket_name = var.terraform_bucket_name
  depends_on                  = [module.artifact_registry]
}

module "gke" {
  source          = "./gke"
  project_id      = var.project_id
  service_account = google_service_account.account.account_id
  depends_on      = [module.artifact_registry, module.memorystore, google_service_account.account]
}


