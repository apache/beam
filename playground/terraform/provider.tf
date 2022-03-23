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

terraform {
  # this describe buket for save state playground cloud
  backend "gcs" {
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.0.0"
    }
  }
}

provider "google" {
  region = var.region
  project = var.project_id
  // TODO may need to run module.setup first independent of this solution and add the terraform service account as a variable
  // This allows us to use a service account to provision resources without downloading or storing service account keys
  #  impersonate_service_account = module.setup.terraform_service_account_email
}

// TODO: required by artifact registry and memorystore; remove when generally available
provider "google-beta" {
  region = var.region
  project = var.project_id
  // TODO may need to run module.setup first independent of this solution and add the terraform service account as a variable
  // This allows us to use a service account to provision resources without downloading or storing service account keys
  #  impersonate_service_account = module.setup.terraform_service_account_email
}