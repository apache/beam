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

module "setup" {
  source = "./setup"
  project_id = var.project_id
  service_account_id = var.service_account_id
  depends_on = [module.api_enable]
}

module "functions_buckets" {
  source = "./functions_buckets"
  # project_id = var.project_id
  region      = var.region
  bucket_name = var.bucket_name
  depends_on = [module.setup, module.api_enable, module.firebase]
}

module "api_enable" {
  source = "./api_enable"
  project_id = var.project_id
}

module "cloud_functions" {
  source = "./cloud_functions"
  region = var.region
  project_id = var.project_id
  bucket_name = var.bucket_name
  service_account_id = module.setup.service-account-email
  source_archive_bucket = module.functions_buckets.functions-bucket-name
  source_archive_object = module.functions_buckets.function-bucket-object
  depends_on = [module.functions_buckets, module.setup, module.api_enable, module.firebase]
}

module "firebase" {
  source = "./firebase"
  project_id = var.project_id
  region = var.region
  hosting_site_id = var.hosting_site_id
  depends_on = [module.api_enable, module.setup]
}