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

This directory depends on [ko.build](https://ko.build/) assumes the
working directory is at
[.test-infra/pipelines](../../../..).

## Local Deployment

To deploy on a local kubernetes cluster, such as minikube:
1. Set the `KO_DOCKER_REPO` environment variable.
    ```
    export KO_DOCKER_REPO=ko.local
    ```
2. Build the
[.test-infra/pipelines/src/main/go/cmd/api_overuse_study/refresher](../../../../src/main/go/cmd/api_overuse_study/refresher)
image, making sure to use the `-B` and `-P` flags.
    ```
    ko build -B -P ./src/main/go/cmd/api_overuse_study/refresher
    ```
3. Deploy using ko, again making sure to use the `-B` and `-P` flags.
    ```
    ko apply -B -P -f infrastructure/03.io/api-overuse-study/03.deployment
    ```

## Remote Deployment

To be written in a future PR.