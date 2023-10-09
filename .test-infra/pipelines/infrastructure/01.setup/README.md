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

This directory sets up the Google Cloud project environment for Dataflow usage.

# List of all provision GCP resources

The following table lists all provisioned resources and their rationale.

| Resource                        | Reason                                     |
|---------------------------------|--------------------------------------------|
| API services                    | Required by GCP to provision resources     |
| Dataflow Worker Service Account | Use GCP service account other than default |
| Worker IAM Roles                | Follow principle of least privilege        |
| Artifact Registry Repository    | Required to store template artifacts       |
| Google Cloud Storage bucket     | Required for various storage needs         |

# Usage

Follow terraform workflow convention to apply this module. It assumes the
working directory is at
[.test-infra/pipelines](../..).

## Terraform Init

This module uses a Google Cloud Storage bucket backend.

Initialize the terraform workspace for the `apache-beam-testing` project:

```
DIR=infrastructure/01.setup
terraform -chdir=$DIR init -backend-config=apache-beam-testing.tfbackend
```

or for your own Google Cloud project:

```
DIR=infrastructure/01.setup
terraform -chdir=$DIR init -backend-config=path/to/your/backend-config-file.tfbackend
```

where your `backend-config-file.tfbackend` contains:

```
bucket = <Google Cloud Storage Bucket Name>
```

## Terraform Apply

Notice the `-var-file` flag referencing [common.tfvars](common.tfvars) that
provides opinionated variable defaults.

For `apache-beam-testing`:

```
DIR=infrastructure/01.setup
terraform -chdir=$DIR apply -var-file=common.tfvars -var-file=apache-beam-testing.tfvars
```

or for your own Google Cloud project:

```
DIR=infrastructure/01.setup
terraform -chdir=$DIR apply -var-file=common.tfvars
```
