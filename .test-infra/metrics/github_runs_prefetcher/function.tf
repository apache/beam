resource "google_cloudfunctions_function" "function" {
  name        = "vdjerek_function_test"
  description = "Test prefetch workflow run data"
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