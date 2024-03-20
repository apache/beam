# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_cloudfunctions_function" "prefetcher" {
  name        = "github_workflow_prefetcher"
  description = "Fetchers Github workflow run data and stores in CloudSQL"
  runtime     = "python311"

  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = google_storage_bucket_object.archive.name
  trigger_http          = true
  entry_point           = "github_workflows_dashboard_sync"
  service_account_email = "beam-metrics-posgresql-kube@apache-beam-testing.iam.gserviceaccount.com"

  available_memory_mb   = 2048
  timeout               = 538
  

  environment_variables = {
    "GH_NUMBER_OF_WORKFLOW_RUNS_TO_FETCH" = "30"
  }

  secret_environment_variables {
    key = "GH_APP_ID" 
    secret = "gh-app_id"
    version = "latest"
  }
   secret_environment_variables {
    key = "GH_PEM_KEY" 
    secret = "gh-pem_key"
    version = "latest"
  }
    secret_environment_variables {
    key = "GH_APP_INSTALLATION_ID" 
    secret = "gh-app_installation_id"
    version = "latest"
  }
    secret_environment_variables {
    key = "DB_DBNAME" 
    secret = "github_actions_workflows_db_name"
    version = "latest"
  }
    secret_environment_variables {
    key = "DB_DBUSERNAME" 
    secret = "github_actions_workflows_db_user"
    version = "latest"
  }
    secret_environment_variables {
    key = "DB_DBPWD" 
    secret = "github_actions_workflows_db_pass"
    version = "latest"
  }
    secret_environment_variables {
    key = "DB_PORT" 
    secret = "github_actions_workflows_db_port"
    version = "latest"
  }
    secret_environment_variables {
    key = "DB_HOST" 
    secret = "github_actions_workflows_db_host"
    version = "latest"
  }
}


resource "google_cloud_scheduler_job" "job" {
  name             = "github_workflow_prefetcher-scheduler"
  description      = "Trigger the ${google_cloudfunctions_function.prefetcher.name} Cloud Function every 3 hours."
  schedule         = "0 */3 * * *" # Every 3 hrs
  time_zone        = "Europe/Dublin"
  attempt_deadline = "320s"

  http_target {
    http_method = "GET"
    uri         = google_cloudfunctions_function.prefetcher.https_trigger_url

    oidc_token {
      service_account_email = google_service_account.service_account.email
    }
  }
}