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

# Overview

This directory organizes Infrastructure-as-Code to provision dependent resources for Cloud Build and Playground.

## Requirements:

- [GCP project](https://cloud.google.com/)
- Enabled [Cloud Build API](https://cloud.google.com/apis/docs/getting-started#enabling_apis)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing Google Cloud Storage Bucket to save Terraform state - `state_bucket`
- [Terraform](https://www.terraform.io/)

## Usage

The folders are named according to the required order of execution. The steps below assume you are using a terminal and are in the root directory of this repository.

### 1. Setup the Google Cloud project

The **cloudbuild-manual-setup/01.setup** provisions required dependencies on Google Cloud.

This step provisions:

- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account

### Execute the module

```console
# Create new configuration to auth to GCP Project
foo@bar:~$ gcloud init

# Acquire new user credentials to use for Application Default Credentials
foo@bar:~$ gcloud auth application-default login

# Navigate to cloudbuild-manual-setup/01.setup folder
foo@bar:~$ cd 01.setup

# Setup Cloud Build SA
foo@bar:~$ terraform init
foo@bar:~$ terraform apply

```
### 2. Setup the Google Cloud Build triggers

The **cloudbuild-manual-setup/02.builders** provisions Cloud Build trigger for the builder.

This step provisions:

- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account

### IMPORTANT: DO THIS FIRST:

**Connect Cloud Build and GitHub repository**


1. Fork this repository into your own GitHub organization or personal account

   See documentation and example on forking a repository on GitHub

   https://docs.github.com/en/get-started/quickstart/fork-a-repo

3. Connect forked repository to Cloud Build

   Navigate to Cloud Build to connect your GitHub forked repository.

   https://console.cloud.google.com/cloud-build/triggers/connect

   **IGNORE "Create a Trigger" STEP. This module will build the trigger step**

   See: https://cloud.google.com/build/docs/automating-builds/create-manage-triggers#connect_repo for more details.

*More details:* [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github)

### Execute the module

Set required variables for GitHub repository owner, name and branch.

```
GITHUB_REPOSITORY_OWNER=<CHANGEME>
GITHUB_REPOSITORY_NAME=<CHANGME>
GITHUB_REPOSITORY_BRANCH=<CHANGME>
```
Run the following commands to execute the module.
Terraform will ask your permission before provisioning resources.
If you agree with terraform provisioning resources,
type `yes` to proceed.

```
# Navigate to cloudbuild-manual-setup/02.builders folder
foo@bar:~$ cd 02.builders
foo@bar:~$ terraform init
foo@bar:~$ terraform apply -var="github_repository_owner=$GITHUB_REPOSITORY_OWNER" / 
-var="github_repository_name=$GITHUB_REPOSITORY_NAME" -var="github_repository_branch=$GITHUB_REPOSITORY_BRANCH"
```

After completing these steps, GCP project will have Cloud Build trigger that can be used to build and deploy Beam Playground.


### Run Cloud Build Trigger

To run the trigger you need to execute it manually.

Navigate to https://console.cloud.google.com/cloud-build/triggers.

You should see a "Playground-infrastructure-trigger" Cloud Build trigger listed.

Click the `RUN` button.

See https://cloud.google.com/build/docs/automating-builds/create-manual-triggers?hl=en#running_manual_triggers
for more information.

This step will take nearly 40 minutes to deploy Playground Infrastructure.
