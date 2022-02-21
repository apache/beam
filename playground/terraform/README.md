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

# Requirements

The following items need to be setup for the Playground cluster deployment on GCP:

* [GCP account](https://cloud.google.com/)
* [`gcloud` command-line tool](https://cloud.google.com/sdk/gcloud) and required setup i.e. login
* [Terraform](https://www.terraform.io/downloads.html) tool
* [Docker](https://www.docker.com/get-started)

# Deployment steps

## 0. Create GCS bucket for state

```bash
$ gsutil mb -p ${PROJECT_ID} gs://state-bucket-name
$ gsutil versioning set on gs://state-bucket-name
```

## 1. Provision infrastructure

To deploy Playground infrastructure follow [README.md](./infrastructure/README.md) for infrastructure module.

```bash
./gradlew playground:terraform:InitInfrastructure -Pproject_id="project-id" -Pproject_environment="env-name" -Pdocker-tag="tag-name"
```

## 2. Deploy application

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev
  ```

```bash
./gradlew playground:terraform:deployBackend -Pproject_id="project-id" -Pproject_environment="env-name" -Pdocker-tag="tag-name"
```

```bash
./gradlew playground:terraform:deployFrontend -Pproject_id="project-id" -Pproject_environment="env-name" -Pdocker-tag="tag-name"
```