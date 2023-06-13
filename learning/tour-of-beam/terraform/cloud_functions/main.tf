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

# GCP Cloud Functions that will serve as a part of the backend of Tour of Beam infrastructure
resource "google_cloudfunctions_function" "cloud_function" {
  count                 = length(var.entry_point_names)
  name                  = "${var.environment}_${var.entry_point_names[count.index]}"
  runtime               = "go116"
  available_memory_mb   = 128
  project               = var.project_id
  service_account_email = var.cf-service-account-id
  source_archive_bucket = var.source_archive_bucket
  source_archive_object = var.source_archive_object
  region                = var.region
  ingress_settings      = "ALLOW_ALL"
  # Get the source code of the cloud function as a Zip compression
  trigger_http = true
  # Name of the function that will be executed when the Google Cloud Function is triggered
  entry_point = var.entry_point_names[count.index]

  environment_variables = {
    DATASTORE_PROJECT_ID=var.project_id
    GOOGLE_PROJECT_ID=var.project_id
    PLAYGROUND_ROUTER_HOST=var.pg_router_host
    DATASTORE_NAMESPACE=var.datastore_namespace
  }

  timeouts {
    create = "20m"
    delete = "20m"
  }

}

# Create IAM entry so all users can invoke the cloud functions

# Endpoints serve content only
# Has additional firebase authentication called "Bearer token" for endpoints that update or delete user progress
resource "google_cloudfunctions_function_iam_member" "invoker" {
  count          = length(google_cloudfunctions_function.cloud_function)
  project        = var.project_id
  region         = var.region
  cloud_function = google_cloudfunctions_function.cloud_function[count.index].name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"

  depends_on = [google_cloudfunctions_function.cloud_function]
}
