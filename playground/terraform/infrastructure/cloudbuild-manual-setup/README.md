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

- [GCP project](https://cloud.google.com/)
- Enabled [Cloud Build API](https://cloud.google.com/apis/docs/getting-started#enabling_apis)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing Google Cloud Storage Bucket to save Terraform state - `state_bucket`
- And 2nd existing GCP Bucket to save Cloud build logs 
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## Usage

The folders are named according to the required order of execution.

### 1. Setup the Google Cloud Build  for your GCP project

The `cloudbuild-manual-setup/01.setup` provisions dependencies required to setup Cloud Build for Playground:
- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account

#### Execute the module

```console
# Create new configuration to authenticate to GCP Project
gcloud init

# Acquire new user credentials to use for Application Default Credentials
gcloud auth application-default login

# Navigate to cloudbuild-manual-setup/01.setup folder
cd 01.setup

# Run terraform scripts
terraform init
terraform plan
terraform apply

```
### 2. Connect GitHub repository and Cloud Build
Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect GitHub repository and Cloud Build.

### 3. Setup the Google Cloud Build triggers

The `cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build trigger to build and deploy Beam Playground.

#### Execute the module

Set required variables for GitHub repository owner, name and branch.

```
GITHUB_REPOSITORY_OWNER=<CHANGEME>
GITHUB_REPOSITORY_NAME=<CHANGME>
GITHUB_REPOSITORY_BRANCH=<CHANGME>
```
Run the following commands to execute the module. Terraform will ask your permission before provisioning resources. If you agree with terraform provisioning resources, type `yes` to proceed.

```
# Navigate to cloudbuild-manual-setup/02.builders folder from cloudbuild-manual-setup/01.setup
cd ../02.builders
# Run terraform scripts
terraform init
terraform plan -var="github_repository_owner=$GITHUB_REPOSITORY_OWNER" -var="github_repository_name=$GITHUB_REPOSITORY_NAME" -var="github_repository_branch=$GITHUB_REPOSITORY_BRANCH"
terraform apply -var="github_repository_owner=$GITHUB_REPOSITORY_OWNER" -var="github_repository_name=$GITHUB_REPOSITORY_NAME" -var="github_repository_branch=$GITHUB_REPOSITORY_BRANCH"
```

After completing these steps, 2 Cloud Build triggers will be created in your GCP project that could be used to build and deploy Beam Playground.

### Run Cloud Build Triggers

## Requirement:

To begin deploying Playground using Cloud Build triggers you must first execute steps described in
[Prepare deployment configuration](https://github.com/apache/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#prepare-deployment-configuration).

#### After that, two triggers will have to be executed in sequence.

## Trigger 1 - Playground-infrastructure-trigger:

To run FIRST trigger,
navigate to Cloud Build page of your GCP project and click `RUN` for **Playground-infrastructure-trigger**.

Once Playground infrastructure has been deployed, please navigate to
[Playground deployment README](https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#deploy-playground-infrastructure) and execute step #2:

`Add following DNS A records for the discovered static IP address`

## Trigger 2 - Playground-to-gke-trigger:

Once Playground infrastructure deployed, you could now deploy Playground to GKE.
To run SECOND trigger,
navigate to Cloud Build page of your GCP project and click `RUN` for **Playground-to-gke-trigger**.

Once Playground has been deployed to GKE, please navigate to [Validation](https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#validate-deployed-playground) to perform Playground deployment steps.