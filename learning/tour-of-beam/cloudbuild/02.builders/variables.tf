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

variable "tf_version" {
    default = "1.4.5"
    description = "Terraform version to be installed on cloud build runner"
}

variable "pg_region" {
    description = "Existing Beam Playground's region (e.g: us-west1)"
}

variable "pg_gke_zone" {
    description = "Existing Beam Playground GKE cluster's zone (e.g. us-west1-b)"
}

variable "pg_gke_name" {
    description = "Existing Beam Playground GKE cluster's name"
}

variable "state_bucket" {
    description = "Existing GCS bucket's name to store Terraform state"
}

variable "env_name" {
    description = "Environment name for Tour of Beam backend (e.g. prod, staging). To support multi-environment on same GCP project"
}

variable "tob_region" {
    description = "GCP region where Tour of Beam backend infrastructure will be created"
}

variable "pg_datastore_namespace" {
    description = "Existing Beam Playground's datastore namespace"
}

variable "trigger_source_repo" {
    default = "https://github.com/beamplayground/deploy-workaround"
}

variable "project_id" {
    default = "GCP project id where resources will be created"
}

variable "tourofbeam_deployer_sa_name" {
    default = "https://github.com/beamplayground/deploy-workaround"
    description = "Service account name to be created and used by cloud build"
}

variable "gcp_username" {
    description = "Your username. Can be found in GCP IAM console (e.g. name.surname@example.com)"
}
