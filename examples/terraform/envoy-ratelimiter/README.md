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

## Overview
Apache Beam pipelines often process data at massive scale, which can easily overwhelm external APIs (e.g., Databases, LLM Inference endpoints, SaaS APIs).

This Terraform module deploys a **centralized Rate Limit Service (RLS)** using Envoy. Beam workers can query this service to coordinate global quotas across thousands of distributed workers, ensuring you stay within safe API limits without hitting `429 Too Many Requests` errors.

Example Beam Python Pipelines using it:
*   [Simple DoFn RateLimiter](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/rate_limiter_simple.py)
*   [Vertex AI RateLimiter](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/rate_limiter_vertex_ai.py)

Example Beam Java Pipelines using it:
*   [Simple DoFn RateLimiter](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/RateLimiterSimple.java)

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
   - **Cloud NAT**: Must exist in the region to allow Private Nodes to pull images and reach external APIs. Follow [this](https://docs.cloud.google.com/nat/docs/gke-example#create-nat) for more details.
     **Helper Command** (if you need to create one):
     ```bash
     gcloud compute routers create nat-router --network <VPC_NAME> --region <REGION>
     gcloud compute routers nats create nat-config \
         --router=nat-router \
         --region=<REGION> \
         --auto-allocated-nat-external-ips \
         --nat-all-subnet-ip-ranges
     ```
   - **Validation via Console**:
     1. Go to **Network Services** > **Cloud NAT** in the Google Cloud Console.
     2. Verify a NAT Gateway exists for your **Region** and **VPC Network**.
     3. Ensure it is configured to apply to **Primary and Secondary ranges** (or at least the ranges GKE will use).

# Prepare deployment configuration:
1. Update the `terraform.tfvars` file to define variables specific to your environment:

* `terraform.tfvars` environment variables:
```
project_id            = "my-project-id"             # GCP Project ID
region                = "us-central1"               # GCP Region for deployment
cluster_name          = "ratelimit-cluster"         # Name of the GKE cluster
deletion_protection   = true                        # Prevent accidental cluster deletion (set "true" for prod)
control_plane_cidr    = "172.16.0.0/28"             # CIDR for GKE control plane (must not overlap with subnet)
namespace             = "envoy-ratelimiter"         # Kubernetes namespace for deployment
enable_metrics        = true                        # Enable metrics export to Google Cloud Monitoring
ratelimit_replicas    = 1                           # Initial number of Rate Limit pods
min_replicas          = 1                           # Minimum HPA replicas
max_replicas          = 5                           # Maximum HPA replicas
hpa_cpu_target_percentage        = 75                          # CPU utilization target for HPA (%)
hpa_memory_target_percentage     = 75                          # Memory utilization target for HPA (%)
vpc_name              = "default"                   # Existing VPC name to deploy into
subnet_name           = "default"                   # Existing Subnet name (required for Internal LB IP)
ratelimit_image       = "envoyproxy/ratelimit:e9ce92cc" # Docker image for Rate Limit service
redis_image           = "redis:6.2-alpine"          # Docker image for Redis
ratelimit_resources   = { requests = { cpu = "100m", memory = "128Mi" }, limits = { cpu = "500m", memory = "512Mi" } }
redis_resources       = { requests = { cpu = "250m", memory = "256Mi" }, limits = { cpu = "500m", memory = "512Mi" } }
```

* Custom Rate Limit Configuration (Must override in `terraform.tfvars`):
```
ratelimit_config_yaml = <<EOF
domain: mongo_cps
descriptors:
  - key: database
    value: users
    rate_limit:
      unit: second
      requests_per_unit: 500
EOF
```

# Deploy Envoy Rate Limiter:
1. Initialize Terraform to download providers and modules:
```bash
terraform init
```

2. **Deploy (Recommended)**:
Run the helper script to handle the 
deployment process automatically:
```bash
./deploy.sh
```

3. **Deploy (Manual Alternative)**:
If you prefer running Terraform manually, you must apply in two steps:
```bash
# Step 1: Create Cluster
terraform apply -target=time_sleep.wait_for_cluster

# Step 2: Create Resources
terraform apply
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


# Observability & Metrics:
This module supports native Prometheus metrics export to **Google Cloud Managed Prometheus**.

### Enabling Metrics
 `enable_metrics` is set to `true` by default.

### Available Metrics
Once enabled, the Envoy Rate Limiter exports metrics to Google Cloud Monitoring. You can view them in **Metrics Explorer** by searching for `ratelimit`.

### Sample Metrics
| Metric Name | Description |
| :--- | :--- |
| `ratelimit_service_rate_limit_total_hits` | Total rate limit requests received. |
| `ratelimit_service_rate_limit_over_limit` | Requests that exceeded the limit (HTTP 429). |
| `ratelimit_service_rate_limit_near_limit` | Requests that are approaching the limit. |
| `ratelimit_service_call_should_rate_limit` | Total valid gRPC calls to the service. |

*Note: You will also see many other Go runtime metrics (`go_*`) and Redis client metrics (`redis_*`).*

### Viewing in Google Cloud Console
1. Go to **Monitoring** > **Metrics Explorer**.
2. Click **Select a metric**.
3. Search for `ratelimit` and select **Prometheus Target** > **ratelimit**.
4. Select a metric (e.g., `ratelimit_service_rate_limit_over_limit`) and click **Apply**.
5. Use **Filters** to drill down by `domain`, `key`, or `value` (e.g., `key=database`, `value=users`).

# Clean up resources:
To destroy the cluster and all created resources:

```bash
./deploy.sh destroy
```

Alternatively:
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
|ratelimit_config_yaml  |**Required** Rate Limit configuration content        |-                                |
|region                 |GCP Region for deployment                            |us-central1                      |
|control_plane_cidr     |CIDR block for GKE control plane                     |172.16.0.0/28                    |
|cluster_name           |Name of the GKE cluster                              |ratelimit-cluster                |
|namespace              |Kubernetes namespace to deploy resources into        |envoy-ratelimiter                |
|enable_metrics         |Enable metrics export to Google Cloud Monitoring     |true                             |
|deletion_protection    |Prevent accidental cluster deletion                  |false                            |
|ratelimit_replicas     |Initial number of Rate Limit pods                    |1                                |
|min_replicas           |Minimum HPA replicas                                 |1                                |
|max_replicas           |Maximum HPA replicas                                 |5                                |
|hpa_cpu_target_percentage         |CPU utilization target for HPA (%)                   |75                               |
|hpa_memory_target_percentage      |Memory utilization target for HPA (%)                |75                               |
|ratelimit_image        |Docker image for Rate Limit service                  |envoyproxy/ratelimit:e9ce92cc    |
|redis_image            |Docker image for Redis                               |redis:6.2-alpine                 |
|ratelimit_resources    |Resources for Rate Limit service (map)               |requests/limits (CPU/Mem)        |
|redis_resources        |Resources for Redis container (map)                  |requests/limits (CPU/Mem)        |

