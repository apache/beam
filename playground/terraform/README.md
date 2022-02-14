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

## Requirements and setup

There are following requirements for deployment Playground cluster on GCP using terraform:

* An [GCP account](https://cloud.google.com/) and the [`gcloud`](https://cloud.google.com/sdk/gcloud) command-line tool
* [Terraform](https://www.terraform.io/downloads.html) tool

Authentication is required for the deployment process. `gcloud` tool can be used to login into the GCP account.

```bash
$ gcloud auth login
```

Service account also can be used for authentication using gcloud cli. See more
details [here](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account)

## Installation

Playground deployment requires terraform. There is
required [documentation](https://www.terraform.io/intro/getting-started/install.html) for terraform installation.

Prepare bucket to save terraform state , example  set  PROJECT_ID as 'apache-beam-testing'
```bash
$ gsutil mb -p ${PROJECT_ID} gs://playground-project-state
$ gsutil versioning set on gs://playground-project-state
```

## GCP infrastructure deployment

To deploy Playground infrastructure follow [README.md](infrastructure/README.md) for infrastructure module

## Playground application deployment

Playground require build and push to registry using gradle before apply terraform scripts.

To build and push playground docker image to the Artifact registry, execute `./gradlew playground dockerTagPush` from
Beam repository root

To deploy Playground applications to Cloud App Engine see [README.md](applications/README.md) from infrastructure
module