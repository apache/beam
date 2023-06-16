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

This directory deploys the mock API endpoint needed for the Beam pipelines to
call as part of the overuse study.

# Usage

This directory depends on [ko.build](https://ko.build/) and assumes the
working directory is at
[.test-infra/pipelines](../../../..).

NOTE: If you are using minikube, remember to run the following command first:

```sh
minikube addons enable gcp-auth
```

1. Set `KO_DOCKER_REPO`

[.test-infra/pipelines/infrastructure/01.setup](../../../01.setup) provisioned
a
[Artifact Registry repository](https://cloud.google.com/artifact-registry/docs/repositories),
[ko.build](https://ko.build) will automatically save the built images to the
repository if you set the `KO_DOCKER_REPO` environment variable correctly:

```
export KO_DOCKER_REPO=<region>-docker.pkg.dev/<project>/infra-pipelines
```

where `<region>` is the value set in
[.test-infra/pipelines/infrastructure/01.setup/common.tfvars](../../../01.setup/common.tfvars).
if you setup using
[.test-infra/pipelines/infrastructure/01.setup](../../../01.setup).

1. Build the
   [.test-infra/pipelines/src/main/go/cmd/api_overuse_study/refresher](../../../../src/main/go/cmd/api_overuse_study/refresher)
   image, making sure to use the `-B`, and `-P` flags.
    ```
    ko build -B -P ./src/main/go/cmd/api_overuse_study/refresher
    ```
2. Build the
   [.test-infra/pipelines/src/main/go/cmd/api_overuse_study/echo](../../../../src/main/go/cmd/api_overuse_study/echo)
   image, making sure to use the `-B`, and `-P` flags.
    ```
    ko build -B -P ./src/main/go/cmd/api_overuse_study/echo
    ```
3. Build the
   [.test-infra/pipelines/src/main/go/cmd/api_overuse_study/quota](../../../../src/main/go/cmd/api_overuse_study/quota)
   image, making sure to use the `-B`, and `-P` flags.
    ```
    ko build -B -P ./src/main/go/cmd/api_overuse_study/quota
    ```
4. Initialize Terraform
Initialize the terraform module:
   ```
   DIR=infrastructure/03.io/api-overuse-study/03.deployment
   terraform -chdir=$DIR init
   ```
5. Apply Terraform
Apply the terraform module:
   ```
   DIR=infrastructure/03.io/api-overuse-study/03.deployment
   terraform -chdir=$DIR apply -var-file=common.tfvars -var=image_repository=$KO_DOCKER_REPO
   ```
