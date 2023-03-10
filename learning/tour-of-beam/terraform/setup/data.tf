data "external" "gcloud_account" {
  program = ["gcloud", "config", "get-value", "core/account"]
}