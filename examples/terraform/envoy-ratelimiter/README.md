<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Envoy Rate Limiter on GKE (Terraform)
This directory contains a production-ready Terraform module to deploy a scalable **Envoy Rate Limit Service** on Google Kubernetes Engine (GKE) Autopilot.

## Architectures:
- **GKE Autopilot**: Fully managed, serverless Kubernetes environment.
  - **Private Cluster**: Nodes have internal IPs only.
  - **Cloud NAT (Prerequisite)**: Allows private nodes to pull Docker images.
- **Envoy Rate Limit Service**: A stateless Go/gRPC service that handles rate limit logic.
- **Redis**: Stores the rate limit counters.
- **StatsD Exporter**: Sidecar container that converts StatsD metrics to Prometheus format, exposed on port `9102`.
- **Internal Load Balancer**: A Google Cloud TCP Load Balancer exposing the Rate Limit service internally within the VPC.

## Prerequisites:
### Following items need to be setup for Envoy Rate Limiter deployment on GCP:
1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

2. [Tools Installed](https://cloud.google.com/sdk/docs/install):
   - [Terraform](https://www.terraform.io/downloads.html) >= 1.0
   - [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud`)
   - [kubectl](https://kubernetes.io/docs/tasks/tools/)

3. APIs Enabled:
   ```bash
   gcloud services enable container.googleapis.com compute.googleapis.com
   ```

4. **Network Configuration**:
   - **Cloud NAT**: Must exist in the region to allow Private Nodes to pull images and reach external APIs.
   - **Validation via Console**:
     1. Go to **Network Services** > **Cloud NAT** in the Google Cloud Console.
     2. Verify a NAT Gateway exists for your **Region** (`us-central1`) and **VPC Network**.
     3. Ensure it is configured to apply to **Primary and Secondary ranges** (or at least the ranges GKE will use).

# Prepare deployment configuration:
1. Create a `terraform.tfvars` file to define variables specific to your environment:

* `terraform.tfvars` environment variables:
```
project_id            = "my-project-id"             # GCP Project ID
region                = "us-central1"               # GCP Region for deployment
cluster_name          = "ratelimit-cluster"         # Name of the GKE cluster
deletion_protection   = true                        # Prevent accidental cluster deletion (set "true" for prod)
control_plane_cidr    = "172.16.0.0/28"             # CIDR for GKE control plane (must not overlap with subnet)
ratelimit_replicas    = 1                           # Initial number of Rate Limit pods
min_replicas          = 1                           # Minimum HPA replicas
max_replicas          = 5                           # Maximum HPA replicas
hpa_cpu_target        = 80                          # CPU utilization target for HPA (%)
vpc_name              = "default"                   # Existing VPC name to deploy into
subnet_name           = "default"                   # Existing Subnet name (required for Internal LB IP)
ratelimit_image       = "envoyproxy/ratelimit:e9ce92cc" # Docker image for Rate Limit service
redis_image           = "redis:6.2-alpine"          # Docker image for Redis
```

* Custom Rate Limit Configuration (Must override in `terraform.tfvars`):
```
ratelimit_config_yaml = <<EOF
domain: edge_proxy
descriptors:
  - key: remote_address
    rate_limit:
      unit: second
      requests_per_unit: 50
EOF
```

# Deploy Envoy Rate Limiter:
1. Initialize Terraform to download providers and modules:
```bash
terraform init
```

2. Plan and apply the changes:
```bash
terraform plan -out=tfplan
terraform apply tfplan
```

3. Connect to the service:
After deployment, get the **Internal** IP address:
```bash
terraform output load_balancer_ip
```
The service is accessible **only from within the VPC** (e.g., via Dataflow workers or GCE instances in the same network) at `<INTERNAL_IP>:8081`.

4. **Test with Dataflow Workflow**:
   Verify connectivity and rate limiting logic by running the example Dataflow pipeline.

   ```bash
   # Get the Internal Load Balancer IP
   export RLS_IP=$(terraform output -raw load_balancer_ip)

   python sdks/python/apache_beam/examples/rate_limiter_simple.py \
     --runner=DataflowRunner \
     --project=<YOUR_PROJECT_ID> \
     --region=<YOUR_REGION> \
     --temp_location=gs://<YOUR_BUCKET>/temp \
     --staging_location=gs://<YOUR_BUCKET>/staging \
     --job_name=ratelimit-test-$(date +%s) \
     # Point to the Terraform-provisioned Internal IP
     --rls_address=${RLS_IP}:8081 \
     # REQUIRED: Run workers in the same private subnet
     --subnetwork=regions/<YOUR_REGION>/subnetworks/<YOUR_SUBNET_NAME> \
     --no_use_public_ips
   ```


# Clean up resources:
To destroy the cluster and all created resources:
```bash
terraform destroy
```
*Note: If `deletion_protection` was enabled, you must set it to `false` in `terraform.tfvars` before destroying.*

# Variables description:

|Variable               |Description                                          |Default                          |
|-----------------------|:----------------------------------------------------|:--------------------------------|
|project_id             |**Required** Google Cloud Project ID                 |-                                |
|vpc_name               |**Required** Existing VPC name to deploy into        |-                                |
|subnet_name            |**Required** Existing Subnet name                    |-                                |
|region                 |GCP Region for deployment                            |us-central1                      |
|control_plane_cidr     |CIDR block for GKE control plane                     |172.16.0.0/28                    |
|cluster_name           |Name of the GKE cluster                              |ratelimit-cluster                |
|deletion_protection    |Prevent accidental cluster deletion                  |false                            |
|ratelimit_replicas     |Initial number of Rate Limit pods                    |1                                |
|min_replicas           |Minimum HPA replicas                                 |1                                |
|max_replicas           |Maximum HPA replicas                                 |5                                |
|hpa_cpu_target         |CPU utilization target for HPA (%)                   |80                               |
|ratelimit_image        |Docker image for Rate Limit service                  |envoyproxy/ratelimit:e9ce92cc    |
|redis_image            |Docker image for Redis                               |redis:6.2-alpine                 |

