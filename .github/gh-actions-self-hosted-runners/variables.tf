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


variable "ORG_NAME" {
  type        = string
  description = "GitHub Organization"
}

variable "TOKEN_CLOUD_FUNCTION" {
  type        = string
  description = "URL for token retrieval"
}

variable "instance_scale_values" {
  type        = map(any)
  description = "# of max, min and desired instances for autoscaling"

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

variable "node_count" {
  type        = number
  description = "Node count for linux K8s cluster"
}

variable "initial_node_count" {
  type        = number
  description = "Initial node count for linux k8s cluster"
}

variable "max_node_count" {
  type        = number
  description = "Maximum number of nodes for the cluster autoscaling"
}

variable "windowsPrefix" {
  type        = string
  description = "Prefix for windows resources"
}


variable "windows_vm_machine_type" {
  type        = string
  description = "Windows machine type"
}

variable "mig_cooldown" {
  type        = number
  description = "Cooldown for VMs in the MIG"
}

variable "windows_runner_template" {
  type        = string
  description = "Instance template for Windows Runner"
}

variable "timeout" {
  type        = number
  description = "Timeout for creation and update"
}

variable "disk_size" {
  type        = number
  description = "Disk size for windows image"
}

variable "source_image" {
  type        = string
  description = "Image disk for Windows Self-Hosted Runners"
}


variable "target_cpu_utilization" {
  type        = number
  description = "CPU percentage for autoscaling"
}
