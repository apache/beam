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
- [Terraform](https://www.terraform.io/)

## Usage

The folders are named according to the required order of execution. The steps below assume you are using a terminal and are in the root directory of this repository.

### 1. Setup the Google Cloud project

The `cloudbuild-manual-setup/01.setup` provisions dependencies required to setup Cloud Build for Playground:
- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account

#### Execute the module

```console
# Create new configuration to auth to GCP Project
gcloud init

# Acquire new user credentials to use for Application Default Credentials
gcloud auth application-default login

# Navigate to cloudbuild-manual-setup/01.setup folder
cd 01.setup

# Setup Cloud Build SA
terraform init
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
terraform init
terraform apply -var="github_repository_owner=$GITHUB_REPOSITORY_OWNER" /
-var="github_repository_name=$GITHUB_REPOSITORY_NAME" -var="github_repository_branch=$GITHUB_REPOSITORY_BRANCH"
```

After completing these steps, GCP project will have `Playground-infrastructure-trigger` Cloud Build trigger that can be used to build and deploy Beam Playground.


### Run Cloud Build Trigger

To run the trigger, execute the trigger manually. Navigate to https://console.cloud.google.com/cloud-build/triggers and click `Playground-infrastructure-trigger` `RUN` button. This step will may take 40 minutes to deploy Playground Infrastructure.

See [Running manual triggers](https://cloud.google.com/build/docs/manually-build-code-source-repos?hl=en#running_manual_triggers) for more information.