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

locals {
  functions = [
    {
      name        = "playground-function-cleanup-${var.env}"
      description = "Playground function cleanup-${var.env}"
      entry_point = "cleanupSnippets"
    },
    {
      name        = "playground-function-put-${var.env}"
      description = "Playground function put-${var.env}"
      entry_point = "putSnippet"
    },
    {
      name        = "playground-function-view-${var.env}"
      description = "Playground function view-${var.env}"
      entry_point = "incrementSnippetViews"
    },
  ]
}

resource "google_cloudfunctions_function" "playground_functions" {
  count                   = length(local.functions)
  name                    = local.functions[count.index].name
  description             = local.functions[count.index].description
  entry_point             = local.functions[count.index].entry_point
  ingress_settings        = "ALLOW_INTERNAL_ONLY"
  runtime                 = "go120"
  source_archive_bucket   = var.gkebucket
  source_archive_object   = "cloudfunction.zip"
  trigger_http            = true
  timeout                 = "540"
  available_memory_mb     = 2048
  service_account_email   = var.service_account_email_cf
  environment_variables   = {
    GOOGLE_CLOUD_PROJECT  = var.project_id
  }
}

resource "google_cloudfunctions_function_iam_member" "invokers" {
  count         = length(local.functions)
  project       = var.project_id
  region        = var.region
  cloud_function = google_cloudfunctions_function.playground_functions[count.index].name
  role          = "roles/cloudfunctions.invoker"
  member        = "allUsers"
}