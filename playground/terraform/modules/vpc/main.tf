resource "google_compute_network" "playground_vpc" {
  project                 = "${var.project_id}"
  name                    = "${var.vpc_name}"
  auto_create_subnetworks = "${var.create_subnets}"
  mtu                     = "${var.mtu}"
}
