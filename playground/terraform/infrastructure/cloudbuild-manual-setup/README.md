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

This directory organizes Infrastructure-as-Code to provision dependent resources and setup Cloud Build for Beam Playground.

## Requirements:

- [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least following privileges:
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
    - Security Admin
    - Service Account User
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing GCP Bucket to save Terraform state - `state_bucket`
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## 1. Setup the Google Cloud Build  for your GCP project

The `playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to setup Cloud Build for Playground:
- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account
- GCP Storage Bucket for Cloud Build logs (Note: [name must be globally unique](https://cloud.google.com/storage/docs/buckets#:~:text=Bucket%20names%20reside%20in%20a,responds%20with%20an%20error%20message.))

#### To execute the module:

1. Create a folder `playground/terraform/environment/{environment_name}` (further referred as `environment_name`) to define a new environment for Playground.
2. Create `terraform.tfvars` file in newly created `environment_name` folder.
3. Fill the `terraform.tfvars` configuration file as follows:

```
project_id                    = "project_id"                # Your Project ID
region                        = "us-central1"               # Your GCP region name where resources will be provisioned
location                      = "us-central1-b"             # Select the deployment location from available in the specified region
cloudbuild_service_account_id = "cloudbuild_sa_name"        # Name of SA to be used by Cloud Build to deploy Playground
cloudbuild_logs_bucket_name   = "logs_bucket_name"          # Assign globally unique name of GCP bucket for Cloud Build logs
github_repository_name        = "beam"                      # The name of the GitHub repo. Example: In https://github.com/example/foo is 'foo'
github_repository_owner       = "owner_name"                # Owner of the GitHub repo. Example: In https://github.com/example/foo is 'example'.
github_repository_branch      = "branch_name"               # The name of GitHub repo branch

network_name                  = "network_name"              # GCP VPC Network Name for Playground deployment
gke_name                      = "playground-backend"        # Playground GKE Cluster name
state_bucket                  = "bucket_name"               # GCS bucket name for Beam Playground temp files
bucket_examples_name          = "bucket_name-example"       # GCS bucket name for Playground examples storage
```

**Note:** Some regions can be prohibited for Cloud Build. Please see [Cloud Build locations](https://cloud.google.com/build/docs/locations) for supported locations.

4. Run commands:

```console
# Create new authentication configuration to GCP Project with created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup folder
cd playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/

# Run terraform scripts
terraform init -backend-config="bucket=state_bucket"
terraform plan -var-file="../../environment/{environment_name}/terraform.tfvars"
terraform apply -var-file="../../environment/{environment_name}/terraform.tfvars"
```

## 2. Connect GitHub repository and Cloud Build
Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect GitHub repository and Cloud Build.

## 3. Setup the Google Cloud Build triggers

The `playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build triggers to build and deploy Beam Playground.

#### To execute the module

```
# Navigate to playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders folder
cd ../02.builders

# Run terraform scripts
terraform init -backend-config="bucket=state_bucket"
terraform plan -var-file="../../environment/{environment_name}/terraform.tfvars"
terraform apply -var-file="../../environment/{environment_name}/terraform.tfvars"
```

## 4. Running first Cloud Build trigger to deploy Playground infrastructure

1. Perform steps described in [Prepare deployment configuration](https://github.com/apache/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#prepare-deployment-configuration)
2. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (Example: us-central1).
3. Open Trigger: `Playground-infrastructure-trigger`.
4. Scroll down to `Advanced` - `Substitutions variables` and Click on `+ ADD VARIABLE`.
5. Assign values for next variables:
    - `_ENVIRONMENT_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; # *Created "environment_name"*
    - `_DNS_NAME`  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your DNS record name*
    - `_LOGS_BUCKET_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GCP logs bucket name (configured in terraform.tfvars)*
6. Click `Save` and Run the trigger `Playground-infrastructure-trigger`.

7. Once Playground infrastructure has been deployed, please navigate to
   [Playground deployment README](https://github.com/apache/beam/tree/master/playground/terraform#deploy-playground-infrastructure) and execute step #2:
   `Add following DNS A records for the discovered static IP address`

## 5. Running second Cloud Build trigger to deploy Playground to GKE

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (Example: us-central1).
2. Open Trigger: `Playground-to-gke-trigger`.
3. Scroll down to `Advanced` - `Substitutions variables` and Click on `+ ADD VARIABLE`.
4. Assign values for next variables:
    - `_ENVIRONMENT_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  # *Created "environment_name"*
    - `_DNS_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  &nbsp;  &nbsp;  *# Your DNS record name*
    - `_TAG` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Tag name for your Playground container images*
    - `_GKE_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GKE cluster name (configured in terraform.tfvars)*
    - `_LOGS_BUCKET_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GCP logs bucket name (configured in terraform.tfvars)*
5. Click `Save` and Run the trigger `Playground-to-gke-trigger`.

## 6. Validation

Once Playground has been deployed to GKE, please navigate to [Validation](https://github.com/apache/beam/tree/master/playground/terraform#validate-deployed-playground) to perform Playground deployment steps.