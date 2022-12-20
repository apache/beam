provider "google" {
  project = var.project
  region  = var.region
  zone = var.zone
}

resource "google_container_cluster" "gh-actions-linux-cluster" {
  name                     = var.prefix
  remove_default_node_pool = true
  initial_node_count       = var.initial_node_count
  location = var.zone

  maintenance_policy {
          recurring_window {
               end_time   = "2022-12-13T06:00:00Z" 
               recurrence = "FREQ=WEEKLY;BYDAY=SA" 
               start_time = "2022-12-12T06:00:00Z" 
            }
        }
    
  
  addons_config {
    horizontal_pod_autoscaling {
      disabled = false
    }
  }

}

resource "google_container_node_pool" "preemptible_nodes" {
  name       = "${var.prefix}-pool"
  cluster    = google_container_cluster.gh-actions-linux-cluster.id
  node_count = var.node_count
  location = var.zone
  
  node_config {
    preemptible     = true
    machine_type    = var.nodes_machine_type
    service_account = var.nodes_service_account_email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    disk_size_gb = var.nodes_disk_size
  }
}