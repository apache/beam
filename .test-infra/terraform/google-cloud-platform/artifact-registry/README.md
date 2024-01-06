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

This directory holds code to provision IAM resources using [terraform](https://terraform.io) to support Apache Beam
testing.

# Requirements and Usage

See [Google Cloud Platform requirements](../../google-cloud-platform/README.md)
for details on requirements and usage.

# Prerequisites

This module assumes the following pre-existing resources:

- [Cloud Resource Manager API Enabled](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com)

# Step 1. Create vars.tfvars

## If you are provisioning in `apache-beam-testing`:

Create a `*.tfvars` file with the recommended naming convention:
`<artifact-registry-id-prefix>.apache-beam-testing.tfvars`, where the `artifact-registry-id-prefix`
is the prefix to be assigned to the service account ID provisioned by this module.
See [wasmx.apache-beam-testing.tfvars](wasmx.apache-beam-testing.tfvars) as an example.

## If you are provisioning in a custom GCP project:

Create a `vars.tfvars` file
in [.test-infra/terraform/google-cloud-platform/artifact-registry](.).
Edit with your IDE terraform plugin installed and it will autocomplete the
variable names.

# Step 2. Create .tfbackend file

Create a new file to specify the
[backend partial configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration#partial-configuration).

## Name the file to communicate intent.

If using `apache-beam-testing`, a suggested naming convention is
`.<artifact-registry-id-prefix>.apache-beam-testing.tfbackend`. *Note this is conventionally a dot file but doesn't have to be.*

For example, [.wasmx.apache-beam-testing.tfbackend](.wasmx.apache-beam-testing.tfbackend) communicates
to others that the backend specifies the state of a service account with an ID prefix `wasmx` in the
`apache-beam-testing` project.

## Configure the .tfbackend file

Follow documented instructions for configuring a state backend's
[partial configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration#partial-configuration).

`apache-beam-testing` uses a
[Google Cloud Storage backend](https://developer.hashicorp.com/terraform/language/settings/backends/gcs), 

Note that the [prefix](https://developer.hashicorp.com/terraform/language/settings/backends/gcs#prefix)
argument should be `terraform/state/github.com/apache/beam/.test-infra/google-cloud-platform/iam/<service account ID prefix>`
For example, [.wasmx.apache-beam-testing.tfbackend](.wasmx.apache-beam-testing.tfbackend) configures a prefix
as `terraform/state/github.com/apache/beam/.test-infra/google-cloud-platform/iam/wasmx`.

# Step 3. Initialize and apply the terraform module.

## If you are provisioning in `apache-beam-testing`:

Assign [backend partial configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration#partial-configuration)
created in the previous step.

```
SERVICE_ACCOUNT_ID_PREFIX=<change me>
```

```
terraform init -backend-config=.$SERVICE_ACCOUNT_ID_PREFIX.apache-beam-testing.tfbackend
terraform apply -var-file=$SERVICE_ACCOUNT_ID_PREFIX.apache-beam-testing.tfvars
```

You will be prompted for any remaining variables.

## If you are provisioning in a custom GCP project:

```
terraform init
terraform apply -var-file=vars.tfvars
```

You will be prompted for any remaining variables.
