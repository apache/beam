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

# Vertex AI Custom Prediction Route Test Setup

To run the `test_vertex_ai_custom_prediction_route` in [vertex_ai_inference_it_test.py](../../vertex_ai_inference_it_test.py), you need a dedicated Vertex AI endpoint with an invoke-enabled model deployed.

## Resource Setup Steps

Run these commands in the `apache-beam-testing` project (or your own test project).

### 1. Build and Push Container

From this directory:

```bash
# on Linux
export PROJECT_ID="apache-beam-testing"  # Or your project
export IMAGE_URI="gcr.io/${PROJECT_ID}/beam-ml/beam-invoke-echo-model:latest"

docker build -t ${IMAGE_URI} .
docker push ${IMAGE_URI}
```

### 2. Upload Model and Deploy Endpoint

Use the Python SDK to deploy (easier than gcloud for specific invocation flags).

```python
from google.cloud import aiplatform

PROJECT_ID = "apache-beam-testing"
REGION = "us-central1"
IMAGE_URI = f"gcr.io/{PROJECT_ID}/beam-ml/beam-invoke-echo-model:latest"

aiplatform.init(project=PROJECT_ID, location=REGION)

# 1. Upload Model with invoke route enabled
model = aiplatform.Model.upload(
    display_name="beam-invoke-echo-model",
    serving_container_image_uri=IMAGE_URI,
    serving_container_invoke_route_prefix="/*",  # <--- Critical for custom routes
    serving_container_health_route="/health",
    sync=True,
)

# 2. Create Dedicated Endpoint (required for invoke)
endpoint = aiplatform.Endpoint.create(
    display_name="beam-invoke-test-endpoint",
    dedicated_endpoint_enabled=True,
    sync=True,
)

# 3. Deploy Model
# NOTE: Set min_replica_count=0 to save costs when not testing
endpoint.deploy(
    model=model,
    traffic_percentage=100,
    machine_type="n1-standard-2",
    min_replica_count=0,
    max_replica_count=1,
    sync=True,
)

print(f"Deployment Complete!")
print(f"Endpoint ID: {endpoint.name}")
```

### 3. Update Test Configuration

1. Copy the **Endpoint ID** printed above (e.g., `1234567890`).
2. Update `_INVOKE_ENDPOINT_ID` in `apache_beam/ml/inference/vertex_ai_inference_it_test.py`.

## Cleanup

To avoid costs, undeploy and delete resources when finished:

```bash
# Undeploy model from endpoint
gcloud ai endpoints undeploy-model <ENDPOINT_ID> --deployed-model-id <DEPLOYED_MODEL_ID> --region=us-central1

# Delete endpoint
gcloud ai endpoints delete <ENDPOINT_ID> --region=us-central1

# Delete model
gcloud ai models delete <MODEL_ID> --region=us-central1
```
