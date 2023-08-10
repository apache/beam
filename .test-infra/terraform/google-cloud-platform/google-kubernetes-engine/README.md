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

# Overview

This module provisions a private Google Kubernetes Engine cluster in the
Google Cloud Platform (GCP).

# Requirements and Usage

See [Google Cloud Platform requirements](../../google-cloud-platform/README.md)
for details on requirements and usage.

# Prerequisites

This module assumes the following pre-existing resources:

- [Cloud Resource Manager API Enabled](https://pantheon.corp.google.com/apis/library/cloudresourcemanager.googleapis.com)
- [Virtual Private Cloud (VPC) network and subnetwork](https://cloud.google.com/vpc/docs/create-modify-vpc-networks)
- [GCP Service Account](https://cloud.google.com/iam/docs/service-accounts-create)

# Step 1. Create vars.tfvars

## If you are provisioning in `apache-beam-testing`:

You can skip this step and follow the next instruction. For security reasons,
the `service_account_id` was omitted.

## If you are provisioning in a custom GCP project:

Create a `vars.tfvars` file
in [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine](.).
Edit with your IDE terraform plugin installed and it will autocomplete the
variable names.

# Step 2. Initialize and apply the terraform module.

## If you are provisioning in `apache-beam-testing`:

```
terraform init
terraform apply -var-file=apache-beam-testing.tfvars
```

You will be prompted for any remaining variables.

## If you are provisioning in a custom GCP project:

```
terraform init
terraform apply -var-file=vars.tfvars
```

You will be prompted for any remaining variables.
