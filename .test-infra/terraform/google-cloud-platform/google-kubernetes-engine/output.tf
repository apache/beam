output "kubernetes_node_service_account" {
  type = object({
    email = string
  })
  value = module.setup.kubernetes_node_service_account
}

output "network" {
  value = module.network.network
}

output "subnetwork" {
  value = module.network.subnetwork
}

output "cluster" {
  value = module.cluster.cluster
}

output "bastion_host" {
  value = module.bastion.bastion_host
}
