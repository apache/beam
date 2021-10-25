variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "vpc_name" {
  description = "Name of VPC to be created"
  default     = "playground-vpc"
}

variable "create_subnets" {
  description = "Auto Create Subnets Inside VPC"
  default = true
}

variable "mtu" {
  description = "MTU Inside VPC"
  default     = 1460
}



