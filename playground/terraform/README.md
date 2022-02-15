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

The following items need to be setup for the Playground cluster deployment on GCP:

* [GCP account](https://cloud.google.com/)
* [`gcloud` command-line tool](https://cloud.google.com/sdk/gcloud)
* [Terraform](https://www.terraform.io/downloads.html) tool
* [Docker](https://www.docker.com/get-started)

### Authentication

Authentication is required for the deployment process. `gcloud` tool can be used to log in into the GCP account:

```bash
$ gcloud auth login
```

Service account also can be used for authentication using `gcloud` cli. See more
details [here](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account).

## First time Playground deployment

### Terraform state storage

GCS bucket is required for Playground deployment that store terraform state, example set PROJECT_ID as '
apache-beam-testing'

```bash
$ gsutil mb -p ${PROJECT_ID} gs://state-bucket-name
$ gsutil versioning set on gs://state-bucket-name
```

This bucket required to deploy applications. Use the same bucket name in related variable.

### GCP infrastructure deployment

To deploy Playground infrastructure run gradle task

```bash
$ cd /path/to/beam
# Pproject_environment might be anything like dev/test/prod and affect to names of services and images
$ ./gradlew playground:terraform:terraformInit  -Pproject_id=gcp-project-id -Pbucket=state-bucket-name -Pproject_environment=environment_name
```

or follow [README.md](./infrastructure/README.md) for deploy infrastructure module via terraform manually.

### Playground application deployment

Playground requires building and pushing to registry using gradle before applying of Terraform scripts.

To build and push Playground Docker image to the [Artifact Registry](https://cloud.google.com/artifact-registry)
from Apache Beam repository root, execute the following commands:

```bash
$ cd /path/to/beam
# Pproject_environment might be anything like dev/test/prod and affect to names of services and images
$ ./gradlew playground:terraform:terraformRef  -Pproject_id=gcp-project-id -Pproject_environment=environment_name 
$ cd playground/terraform && export $DOCKER_REPOSITORY_ROOT==$(terraform output docker-repository-root) && cd -
$ ./gradlew playground:backend:containers:java:dockerTagPush -Pdocker-repository-root=$DOCKER_REPOSITORY_ROOT  -Pdocker-tag=docker-tag
$ ./gradlew playground:backend:containers:python:dockerTagPush -Pdocker-repository-root=$DOCKER_REPOSITORY_ROOT  -Pdocker-tag=docker-tag
$ ./gradlew playground:backend:containers:go:dockerTagPush -Pdocker-repository-root=$DOCKER_REPOSITORY_ROOT -Pdocker-tag=docker-tag
$ ./gradlew playground:backend:containers:scio:dockerTagPush -Pdocker-repository-root=$DOCKER_REPOSITORY_ROOT -Pdocker-tag=docker-tag
$ ./gradlew playground:backend:containers:router:dockerTagPush -Pdocker-repository-root=$DOCKER_REPOSITORY_ROOT -Pdocker-tag=docker-tag
# Frontend TBD
``` 

To deploy Playground applications to Cloud App Engine run gradle task

```bash
$ cd /path/to/beam
# Pproject_environment might be anything like dev/test/prod and affect to names of services and images
$ ./gradlew playground:terraform:terraformApplyApp \
 -Pproject_id=gcp-project-id \
 -Pbucket=state-bucket-name \
 -Pproject_environment=environment_name \
 -Pdocker-tag=docker-tag
```

or see [README.md](./applications/README.md) for deploy applications module via terraform manually.

## Update Playground

To update infrastructure or applications execute following commands

**Update all**

```bash
$ cd /path/to/beam
$ ./gradlew playground:terraform:terraformApply \
  -Pproject_id=gcp-project-id \ 
  -Pproject_environment=dev terraformApply \
  -Pdocker-tag=docker-tag
```
