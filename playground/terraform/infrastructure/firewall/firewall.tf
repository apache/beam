resource "google_compute_firewall" "playground-firewall-deny-egress" {
  name    = "playground-deny-egress"
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
  name    = "playground-allow-controlplane"
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
  name    = "playground-allow-dns"
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
  name    = "playground-allow-privateapi"
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
  name    = "playground-allow-redis"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "all"
  }

  destination_ranges = [var.redis_ip]
  target_tags = ["beam-playground"]
}