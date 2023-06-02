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

# Tour of Beam Cloud Build Setup

This directory organizes Infrastructure-as-Code to provision dependent resources and set up Cloud Build for automated Tour of Beam Backend Infrastructure provisioning, Backend and frontend deployment, manual and automated CI/CD.

## Requirements:

1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. Existing Beam Playground environment/infrastructure deployed in same GCP Project. Tour of Beam will be deployed to that same location  (region/zone). If you don't have one, please follow [Beam Playground deployment instructions](./../../../playground/terraform/infrastructure/cloudbuild-manual-setup/README.md) to deploy it.
3. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):

   - Cloud Datastore Owner
   - Create Service Accounts
   - Security Admin
   - Service Account User
   - Service Usage Admin
   - Storage Admin
   - Kubernetes Engine Cluster Viewer
   - Secret Manager Admin
   - Cloud Build Editor

4. [Google Cloud Storage buckets](https://cloud.google.com/storage/docs/creating-buckets) for:
- Terraform state for Cloud Build triggers: \<tourofbeam-triggers-state-env\>
- Cloud Build private logs: \<tourofbeam-cb-private-logs-env\>
- Cloud Build public logs: \<tourofbeam-cb-public-logs-env\>. Don't enforce public access prevention on this bucket.
- Terraform state for Tour of Beam deployment: \<tourofbeam-deployment-state-env\>

It's advised to add environment name to the bucket name to avoid collisions with other environments.

5. An OS with the following software installed:

* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication plugin](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
* [Git client](https://git-scm.com/downloads)

DEV NOTE: GCP Cloud shell can be used for deployment. It has all required software pre-installed.

6. [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

# Prepare deployment configuration

1. Generate a Terraform variable file called `beam/learning/tour-of-beam/cloudbuild/common.tfvars`. Place the values listed below into the file, adjusting them as needed:

project_id               = "apache-beam-testing"      # GCP project ID
tourofbeam_deploy_sa     = "tourofbeam-cb-deploy-env" # Service account for Initialize-Playground-environment trigger
tourofbeam_ci_sa         = "tourofbeam-cb-ci-env"     # Service account for CI trigger
tourofbeam_cd_sa         = "tourofbeam-cb-cd-env"     # Service account for CD trigger
environment_name         = "env"                      # Environment name
playground_dns_name      = "fqdn.beam.apache.org"     # Playground DNS name
pg_region                = "us-west1"                 # Playground region
pg_gke_zone              = "us-west1-a"               # Playground GKE zone
pg_gke_name              = "cluster_name"             # Playground GKE cluster name
pg_datastore_namespace   = "env"                      # Playground Datastore namespace name
tourofbeam_deployment_state_bucket  = "tourofbeam-deployment-state-env"     # Bucket for Terraform state for deployment
webhook_trigger_secret_id           = "secret-cloudbuild-triggers-webhook"  # Secret ID for webhook trigger
gh_pat_secret_id                    = "github_pat_playground_deployment"    # Secret ID for GitHub PAT
tourofbeam_deploy_trigger_name      = "TourOfBeam-Deploy-Update-env"        # Trigger name for deployment trigger
tourofbeam_ci_trigger_name   = "TourOfBeam-CI-env"                # Trigger name for CI trigger
tourofbeam_cd_trigger_name   = "TourOfBeam-CD-env"                # Trigger name for CD trigger
tourofbeam_cb_private_bucket = "tourofbeam-cb-private-logs-env"   # Bucket for Cloud Build private logs
tourofbeam_cb_public_bucket  = "tourofbeam-cb-public-logs-env"    # Bucket for Cloud Build public logs
web_app_id = "Tour-Of-Beam"  = "tour-of-beam"                     # Web app ID

If you plan to have only one environment, you can use simple resource names like `tourofbeam-deployment-state` or `tourofbeam-cb-private-logs` instead of `tourofbeam-deployment-state-env` or `tourofbeam-cb-private-logs-env`. But if you plan to have multiple environments, it's advised to add environment name to the resource name to avoid collisions with other environments.

## 1. Set up the Google Cloud Build for your GCP project

The `beam/learning/tour-of-beam/cloudbuild/01.setup` provisions dependencies required to set up Cloud Build for Tour of Beam:
- Required API services
- Cloud Build triggers service accounts
- IAM roles for Cloud Build service account

#### To execute the module:

1. Run commands:

```console

# Create a new authentication configuration for GCP Project with the created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup directory
cd beam/learning/tour-of-beam/cloudbuild/01.setup

# Run terraform commands
terraform init -backend-config="bucket=\<tourofbeam-triggers-state-env\>
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"
```

## 2. Connect default (https://github.com/beamplayground/deploy-workaround) GitHub repository with GCP Cloud Build

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect GitHub repository with GCP Cloud Build.

## 3. Set up the Google Cloud Build triggers

The `beam/learning/tour-of-beam/cloudbuild/02.builders` provisions:
- Cloud Build triggers to build and deploy Tour of Beam backend infrastructure

#### To execute the module

```
# Navigate to beam/learning/tour-of-beam/cloudbuild/02.builders directory
cd ../02.builders

# Run terraform commands and provide required values
terraform init -backend-config="bucket=\<tourofbeam-triggers-state-env\>"

terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"

```

## 4. Run Cloud Build trigger to deploy Tour of Beam infrastructure, backend and learning materials

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the global region.
2.  `TourOfBeam-Deploy-Update-env`.
3. Scroll down to `Source` - `Repository` to ensure that Default GitHub repository is connected.
   - Click on drop-down menu and press `CONNECT NEW REPOSITORY` in case it was not automatically connected.
4. Click `Save` and Run the trigger.

## 5. Run Cloud Build trigger to deploy Tour of Beam infrastructure, backend and learning materials

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the global region.
2. Open Trigger: `TourOfBeam-CD-env-manual`.
3. Scroll down to `Source` - `Repository` to ensure that Default GitHub repository is connected.
   - Click on drop-down menu and press `CONNECT NEW REPOSITORY` in case it was not automatically connected.
4. Click `Save` and Run the trigger.

## 6. Validate Tour of Beam backend infrastructure deployment
1. Navigate to Cloud Functions service in GCP.
2. Check if there are cloud functions with prefix of environment (e.g. prod, test) in their names.
3. Navigate to firebase web app url for newly created environment (e.g. https://\<web_app_id\>-\<environment_name\>.web.app).
4. Check if the web app is accessible and all learning materials and examples are working.