resource "random_string" "default" {
  length  = 8
  upper   = false
  special = false
}

resource "google_storage_bucket" "looker_jars" {
  name                        = "${var.storage_bucket_prefix}-${random_string.default.result}"
  location                    = var.storage_bucket_location
  uniform_bucket_level_access = true
}
