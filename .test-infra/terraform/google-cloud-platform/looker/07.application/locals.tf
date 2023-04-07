locals {
  var_dir                             = "/var/looker"
  lookerfiles_path                    = "/mnt/lookerfiles"
  db_credentials_config_name          = "looker-db.yml"
  db_credentials_config_path          = "${local.var_dir}/${local.db_credentials_config_name}"
  looker_persistent_volume_claim_name = "looker-pvc"
}
