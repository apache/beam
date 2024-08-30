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

This directory sets up the Kubernetes environment for subsequent modules.

# Usage

Follow terraform workflow convention to apply this module.
The following assumes the working directory is at
[.test-infra/pipelines/infrastructure/03.io/api-overuse-study](..).

## Terraform Init

Initialize the terraform workspace.

```
DIR=01.setup
terraform -chdir=$DIR init
```

## Terraform Apply

Apply the terraform module.

```
DIR=01.setup
terraform -chdir=$DIR apply -var-file=common.tfvars
```