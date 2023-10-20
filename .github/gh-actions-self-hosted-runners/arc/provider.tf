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
   backend "gcs" {
    prefix = "prod"
   }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "~> 4.62.0"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = ">= 2.0.2"
    }
  }
}

provider "google" {
  region = var.region
  project = var.project_id
}

provider "helm" {
  kubernetes {
    host                    = "https://${google_container_cluster.actions-runner-gke.endpoint}"
    token                   = data.google_client_config.provider.access_token
    cluster_ca_certificate  = base64decode(google_container_cluster.actions-runner-gke.master_auth.0.cluster_ca_certificate)
  }
}

provider "kubectl" {
    host                    = "https://${google_container_cluster.actions-runner-gke.endpoint}"
    token                   = data.google_client_config.provider.access_token
    cluster_ca_certificate  = base64decode(google_container_cluster.actions-runner-gke.master_auth.0.cluster_ca_certificate)
    load_config_file        = false
}
provider "kubernetes" {
    host                    = "https://${google_container_cluster.actions-runner-gke.endpoint}"
    token                   = data.google_client_config.provider.access_token
    cluster_ca_certificate  = base64decode(google_container_cluster.actions-runner-gke.master_auth.0.cluster_ca_certificate)
}
provider "github" {
  app_auth {
    id = data.google_secret_manager_secret_version.github_app_id.secret_data
    pem_file = data.google_secret_manager_secret_version.github_private_key.secret_data
    installation_id = data.google_secret_manager_secret_version.github_app_install_id.secret_data
  }
  owner = var.organization
  
}