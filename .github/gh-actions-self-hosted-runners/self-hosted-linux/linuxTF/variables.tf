variable "prefix" {
  type        = string
  description = "Prefix for all self-hosted runners resources"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "zone" {
  type        = string
  description = "GCP default zone"
}

variable "project" {
  type        = string
  description = "GCP Project id"
}


variable "runners_service_account_email" {
  type        = string
  description = "Service account email for Linux Runners"
}

variable "nodes_service_account_email" {
  type        = string
  description = "Service account email for Linux K8s Nodes"
}

variable "nodes_machine_type" {
  type        = string
  description = "Machine type for K8s nodes"
}

variable "nodes_disk_size" {
  type        = number
  description = "Disk size for Nodes"
}

variable node_count{
  type = number
  description = "Node count for linux K8s cluster"
}

variable initial_node_count{
  type = number
  description = "Initial node count for linux k8s cluster"
}

variable "max_node_count" {
  type = number
  description = "Maximum number of nodes for the cluster autoscaling"  
}

