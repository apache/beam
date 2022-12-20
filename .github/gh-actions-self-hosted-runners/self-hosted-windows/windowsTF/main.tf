

provider "google" {
  project = var.project
  region  = var.region
  #zone    = var.zone
}
# Instance template for the Managed Instance Group
resource "google_compute_instance_template" "windows-github-actions-runner" {
  name        = var.windows_runner_template
  description = "Instance template used by self-hosted GitHub Actions Windows runners"
  tags        = ["v1"]
  labels = {
    os      = "windows"
    version = "windows-server-2019"
  }

  instance_description = "Windows-Server-2019 for GitHub Actions Self-Hosted Runners"
  machine_type         = var.windows_vm_machine_type
  can_ip_forward       = false
  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  disk {
    source_image = var.source_image 
    auto_delete  = true
    boot         = true
    disk_type    = "pd-standard"
    disk_size_gb = var.disk_size

  }
  network_interface {
    network = "default"

    access_config {
      network_tier = "PREMIUM"
    }

  }


  metadata = {
    "windows-startup-script-ps1" = <<-EOF
    Start-Sleep -Seconds 30
    $env:Path += ';C:\Program Files\git\bin' 
    $response= Invoke-RestMethod https://api.github.com/repos/actions/runner/tags
    $version= $response[0].name.substring(1,$response[0].name.Length-1)
    
    $ORG_NAME="${var.ORG_NAME}"
    $ORG_RUNNER_GROUP="Beam"
    $GCP_TOKEN=gcloud auth print-identity-token
    $TOKEN_PROVIDER="${var.TOKEN_CLOUD_FUNCTION}"


    Set-Location C:/

    Write-Output "Starting registration process"

    mkdir "actionsDir"    

    Set-Location "actionsDir"

    Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v$version/actions-runner-win-x64-$version.zip -OutFile actions-runner-win-x64-$version.zip

    Expand-Archive -LiteralPath $PWD\actions-runner-win-x64-$version.zip -DestinationPath $PWD -Force

    $RUNNER_TOKEN=(Invoke-WebRequest -Uri $TOKEN_PROVIDER -Method POST -Headers @{'Accept' = 'application/json'; 'Authorization' = "bearer $GCP_TOKEN"} -UseBasicParsing | ConvertFrom-Json).token

    [System.Environment]::SetEnvironmentVariable('GITHUB_TOKEN', $RUNNER_TOKEN,[System.EnvironmentVariableTarget]::Machine)

    $hostname= "windows-runner-"+[guid]::NewGuid()

    ./config.cmd --name beam-$hostname --token $RUNNER_TOKEN --url https://github.com/$ORG_NAME --ephemeral --work _work --unattended --replace --labels windows,beam,windows-server-2019 --runnergroup $ORG_RUNNER_GROUP

    ./run.cmd
    Stop-Computer -ComputerName localhost
    EOF

    "windows-shutdown-script-ps1" = <<-EOF
    Write-Output "removingRunner"
    Set-Location C:/actionsDir

    $token=[System.Environment]::GetEnvironmentVariable('GITHUB_TOKEN','machine')

    ./config.cmd remove --token $token
    EOF

  }

  service_account {
    email  = var.runners_service_account_email
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_region_instance_group_manager" "mig" {
  name               = "${var.prefix}-instance-group"
  region                     = var.region
  distribution_policy_zones  = ["us-central1-b", "us-central1-f", "us-central1-c"]
  base_instance_name = var.prefix
  version {
    instance_template = google_compute_instance_template.windows-github-actions-runner.self_link
  }


  timeouts {
    create = var.timeout
    update = var.timeout
  }
}

resource "google_compute_region_autoscaler" "autoscaler" {
  provider = google-beta
  name     = "${var.prefix}-autoscaler"
  region = var.region
  project  = var.project
  target   = google_compute_region_instance_group_manager.mig.id

  autoscaling_policy {
    max_replicas    = lookup(var.instance_scale_values, "max", 0)
    min_replicas    = lookup(var.instance_scale_values, "min", 0)
    cooldown_period = var.mig_cooldown

    cpu_utilization {
      target = var.target_cpu_utilization 
    }
  }

}
