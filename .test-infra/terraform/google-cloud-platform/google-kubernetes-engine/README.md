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

# IMPORTANT Prerequisites

This module assumes the following pre-existing resources:

- [Cloud Resource Manager API Enabled](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com)
- [Virtual Private Cloud (VPC) network and subnetwork](https://cloud.google.com/vpc/docs/create-modify-vpc-networks)
- [GCP Service Account](https://cloud.google.com/iam/docs/service-accounts-create) with [minimally permissive IAM roles](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa)
- [Google Cloud Storage Bucket](https://cloud.google.com/storage/docs/creating-buckets) for
[terraform backend](https://developer.hashicorp.com/terraform/language/settings/backends/gcs) configuration;
creation of this resource cannot be automated as part of this module
(`apache-beam-testing` already contains the bucket: `b507e468-52e9-4e72-83e5-ecbf563eda12`)

# Step 1. Create a new .tfbackend file in the [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine](.) directory

Create a new file to specify the
[backend partial configuration](https://developer.hashicorp.com/terraform/language/settings/backends/configuration#partial-configuration).
See below for details on naming convention and expected content.
`apache-beam-testing` usage should be version controlled in this repository.

## Name the file to communicate intent

If using `apache-beam-testing`, a suggested naming convention is
`.<cluster-name-prefix>.apache-beam-testing.tfbackend`. *Note this is conventionally a dot file but doesn't have to be.*

For example, `.my-cluster.apache-beam-testing.tfbackend` communicates
to others that the backend specifies the state of a Kubernetes cluster with an ID prefix `my-cluster` in the
`apache-beam-testing` project.

## Content

The following is the expected content of the `.tfbackend` file, where `bucket` references the name of the
Google Cloud Storage bucket created as a pre-requisite.

See below for details on naming convention and expected content.
`apache-beam-testing` usage should be version controlled in this repository.

If using `apache-beam-testing`:

[.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/copyme.apache-beam-testing.tfbackend](copyme.apache-beam-testing.tfbackend)
contains a template with the following. Just change `<cluster-name-prefix>`.

```
bucket = "b507e468-52e9-4e72-83e5-ecbf563eda12"
prefix = ".test-infra/terraform/google-cloud-platform/google-kubernetes-engine/<cluster-name-prefix>"
```

# Step 2. Create .tfvars file

Create a new file to specify a `.tfvars` file for your new Kubernetes cluster for use with the
[terraform cli](https://developer.hashicorp.com/terraform/cli) `-var-file` flag.
See below for details on naming convention and expected content.
`apache-beam-testing` usage should be version controlled in this repository.

## Name the file to communicate intent

If using `apache-beam-testing`, a suggested naming convention is
`<cluster-name-prefix>.<region>.apache-beam-testing.tfvars`.

For example, `my-cluster.us-central1.apache-beam-testing.tfvars` communicates to others that the

Both
[.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/us-central1.apache-beam-testing.tfvars](us-central1.apache-beam-testing.tfvars)
and
[.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/us-west1.apache-beam-testing.tfvars](us-west1.apache-beam-testing.tfvars)
are example starter `.tfvars` files specific to `apache-beam-testing` for the targeted region.
Editing the `.tfvars` file in an IDE installed with a terraform plugin and it will prompt you for the remaining
variables.

# Step 3. Initialize and apply the terraform module.

Where:
```
CLUSTER_PREFIX=<cluster-id-prefix>
CONFIG=$CLUSTER_PREFIX.apache-beam-testing.tfbackend # file name only without the directory
DIR=.test-infra/terraform/google-cloud-platform/google-kubernetes-engine
VARS=$CLUSTER_PREFIX.us-west1.apache-beam-testing.tfvars
```

Run:
```
terraform -chdir=$DIR init -backend-config=$CONFIG
terraform -chdir=$DIR apply -var-file=$VARS
```

You will be prompted for any remaining variables.
