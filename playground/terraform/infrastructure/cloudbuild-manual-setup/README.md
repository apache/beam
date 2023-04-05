<!---
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

# Beam Playground Cloud Build Setup

This directory organizes Infrastructure-as-Code to provision dependent resources and set up Cloud Build for Beam Playground.
Cloud Build triggers created by terraform scripts from this directory automate steps described in [readme](https://github.com/apache/beam/blob/master/playground/terraform/README.md).

## Requirements:

- [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
    - Security Admin
    - Service Account User
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing GCP Bucket to save Terraform state - `state-bucket`
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## 1. Set up the Google Cloud Build for your GCP project

The `playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to set up Cloud Build for Playground:
- Required API services
- Cloud Build service account
- IAM roles for Cloud Build service account

#### To execute the module:

**Note:**  Please see [Cloud Build locations](https://cloud.google.com/build/docs/locations) for the list of all supported locations.

1. Run commands:


```console
# Set environment variable for state bucket
export STATE_BUCKET="state-bucket"

# Create a new authentication configuration for GCP Project with the created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup directory
cd playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/

# Run terraform commands
terraform init -backend-config="bucket=$STATE_BUCKET"
terraform apply -var="project_id=$(gcloud config get-value project)"
```

## 2. Connect Apache Beam GitHub repository and GCP Cloud Build

**Note:** Ensure correct `region` is set in [Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page before proceeding further.

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect Apache Beam GitHub repository and GCP Cloud Build.

## 3. Set up the Google Cloud Build triggers

The `playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build triggers to build and deploy Beam Playground

#### To execute the module

```
# Navigate to playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders directory
cd ../02.builders

# Run terraform commands
terraform init -backend-config="bucket=$STATE_BUCKET"
terraform apply -var="project_id=$(gcloud config get-value project)" -var="state_bucket=$STATE_BUCKET"
```

## 4. Run Cloud Build `Playground-infrastructure-trigger` to deploy Playground infrastructure

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (In our example: us-central1).
2. Open Trigger: `Playground-infrastructure-trigger`.
3. Scroll down to `Source` - `Repository` to ensure that Apache Beam GitHub repository is connected.
   - Click on drop-down menu and press `CONNECT NEW REPOSITORY` in case it was not automatically connected.
4. Click `Save` and Run the trigger `Playground-infrastructure-trigger`.

5. Once Playground infrastructure has been deployed, please navigate to
   [Playground deployment README](https://github.com/apache/beam/tree/master/playground/terraform#deploy-playground-infrastructure) and execute step #2:
   `Add following DNS A records for the discovered static IP address` expanding use of variable `DNS_NAME`.

## 5. Run Cloud Build `Playground-to-gke-trigger` to deploy Playground to GKE

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (In our example: us-central1).
2. Open Trigger: `Playground-to-gke-trigger`.
3.  Scroll down to `Source` - `Repository` to ensure that Apache Beam GitHub repository is connected.
    - Click on drop-down menu and press `CONNECT NEW REPOSITORY` in case it was not automatically connected.
4. Click `Save` and Run the trigger `Playground-to-gke-trigger`.

## 6. Validate Playground deployment

Once Playground has been deployed to GKE, please navigate to [Validation](https://github.com/apache/beam/tree/master/playground/terraform#validate-deployed-playground) to perform Playground deployment steps.