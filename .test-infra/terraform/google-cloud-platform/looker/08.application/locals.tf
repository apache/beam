locals {
  lookerfiles_path                    = "/mnt/lookerfiles"
  looker_persistent_volume_claim_name = "looker-pvc"

  gcm_key_secret_id       = "gcm-key"
  // https://cloud.google.com/looker/docs/changing-encryption-keys#set_new_environment_variables
  gcm_key_secret_data_key = "LKR_MASTER_KEY_ENV"

  looker_database_credentials_secret_id = "looker-database-credentials"
  // See https://cloud.google.com/looker/docs/migrating-looker-backend-db-to-mysql#create_a_database_credentials_file
  looker_database_credentials_data_key  = "LOOKER_DB"
}
