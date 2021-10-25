output "vpc_name" {
  value = "${google_compute_network.playground_vpc.name}"
}

output "vpc_id" {
  value = "${google_compute_network.playground_vpc.id}"
}
