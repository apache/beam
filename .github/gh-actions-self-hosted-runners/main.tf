provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

terraform {
  backend "gcs" {}
}

module "windowsMIG" {
  source = "./self-hosted-windows/windowsTF"

  prefix                        = var.windowsPrefix
  project                       = var.project
  zone                          = var.zone
  region                        = var.region
  instance_scale_values         = var.instance_scale_values
  runners_service_account_email = var.runners_service_account_email
  ORG_NAME                      = var.ORG_NAME
  TOKEN_CLOUD_FUNCTION          = var.TOKEN_CLOUD_FUNCTION
  windows_vm_machine_type       = var.windows_vm_machine_type
  mig_cooldown                  = var.mig_cooldown
  target_cpu_utilization        = var.target_cpu_utilization
  source_image                  = var.source_image
  disk_size                     = var.disk_size
  timeout                       = var.timeout
  windows_runner_template       = var.windows_runner_template
}


module "linuxK8sCluster" {
  source                        = "./self-hosted-linux/linuxTF"
  project                       = var.project
  zone                          = var.zone
  region                        = var.region
  prefix                        = var.prefix
  nodes_service_account_email   = var.nodes_service_account_email
  runners_service_account_email = var.runners_service_account_email
  nodes_machine_type            = var.nodes_machine_type
  nodes_disk_size               = var.nodes_disk_size
  initial_node_count            = var.initial_node_count
  node_count                    = var.node_count
  max_node_count                = var.max_node_count
}
