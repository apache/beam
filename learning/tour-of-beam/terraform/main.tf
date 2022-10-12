module "setup" {
  depends_on      = [module.setup]
  source             = "./setup"
  project_id         = var.project_id
  service_account_id = var.service_account_id
}


module "buckets" {
  depends_on    = [module.setup]
  source        = "./buckets"
  project_id    = var.project_id
  name          = var.function_bucket_name
  storage_class = var.function_bucket_storage_class
  location      = var.function_bucket_location
}

module "cloud-functions" {
  depends_on = [module.buckets, module.setup]
  source = "./cloud-functions"
  project_id = var.project_id
  service_account_email = module.setup.service-account-email
  region = var.region
}