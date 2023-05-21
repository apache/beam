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

module "infrastructure" {
  source                        = "./infrastructure"
  project_id                    = var.project_id
  region                        = var.region
  environment                   = var.environment
  network_region                = var.region
  redis_region                  = var.region
  location                      = var.zone
  service_account_id            = var.service_account_id
  state_bucket                  = var.state_bucket
  env                           = var.env
  #Artifact Registry
  repository_id                 = var.repository_id
  repository_location           = var.region
  #Redis
  redis_version                 = var.redis_version
  redis_name                    = var.redis_name
  redis_tier                    = var.redis_tier
  redis_replica_count           = var.redis_replica_count
  redis_memory_size_gb          = var.redis_memory_size_gb
  #NETWORK
  network_name                  = var.network_name
  ip_address_name               = var.ip_address_name
  subnetwork_name               = var.subnetwork_name
  #GKE
  gke_machine_type              = var.gke_machine_type
  gke_name                      = var.gke_name
  gke_location                  = var.zone
  service_account               = var.service_account
  skip_appengine_deploy         = var.skip_appengine_deploy
  min_count                     = var.min_count
  max_count                     = var.max_count
}
