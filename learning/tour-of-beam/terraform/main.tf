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

# Setup module to create service accounts, assign required IAM roles to it and deploying user account
module "setup" {
  source = "./setup"
  project_id = var.project_id
  gcloud_init_account = var.gcloud_init_account
  depends_on = [module.api_enable]
}

# GCS buckets to create buckets, objects, archive to store source code that cloud functions will use
module "functions_buckets" {
  source = "./functions_buckets"
  region      = var.region
  depends_on = [module.setup, module.api_enable]
}

# API services module. Enables required APIs for the infrastructure
module "api_enable" {
  source = "./api_enable"
}

# Cloud functions module. Creates cloud functions, as part of Tour of Beam backend infrastructure
module "cloud_functions" {
  source = "./cloud_functions"
  region = var.region
  project_id = var.project_id
  pg_router_host = var.pg_router_host
  environment = var.environment
  datastore_namespace = var.datastore_namespace
  service_account_id = module.setup.service-account-email
  source_archive_bucket = module.functions_buckets.functions-bucket-name
  source_archive_object = module.functions_buckets.function-bucket-object
  depends_on = [module.functions_buckets, module.setup, module.api_enable]
}
