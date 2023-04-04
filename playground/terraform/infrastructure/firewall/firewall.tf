resource "google_compute_firewall" "playground-firewall-deny-egress" {
  name    = "${var.network_name}-deny-egress"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1001
  deny {
    protocol      = "all"
  }
  destination_ranges = ["0.0.0.0/0"]
  target_tags = ["beam-playground"]
}

resource "google_compute_firewall" "playground-firewall-allow-controlplane" {
  name    = "${var.network_name}-allow-controlplane"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol      = "all"
  }
  destination_ranges = [var.gke_controlplane_cidr]
  target_tags = ["beam-playground"]
}

resource "google_compute_firewall" "playground-firewall-allow-dns" {
  name    = "${var.network_name}-allow-dns"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "tcp"
    ports    = ["53"]

  }
  allow {
    protocol = "udp"
    ports = ["53"]
  }
  destination_ranges = ["0.0.0.0/0"]
  target_tags = ["beam-playground"]
}

resource "google_compute_firewall" "playground-firewall-allow-privateapi" {
  name    = "${var.network_name}-allow-privateapi"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "all"
  }

  destination_ranges = ["199.36.153.8/30"]
  target_tags = ["beam-playground"]
}


resource "google_compute_firewall" "playground-firewall-allow-redis" {
  name    = "${var.network_name}-allow-redis"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "all"
  }

  destination_ranges = [var.redis_ip]
  target_tags = ["beam-playground"]
}