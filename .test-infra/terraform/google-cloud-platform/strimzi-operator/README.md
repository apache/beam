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

This module will provision a strimzi operator that will allow to deploy kafka clusters as defined by the strmizi kafka clustom resource. 

# Deployment

# Step 1. Initialize and apply the terraform module.


For initialization make sure you use the `beam-infra-terraform-state` and `clustername-strimzi` prefix.
```
terraform init -backend-config="bucket=beam-infra-terraform-state" -backend-config="prefix=cluastername-strimzi"
```
## Step 2. Plan the module. Make sure that terraform is not replacing or deleting resrouces unless expected:
Also note that by default it will try to use teh default kubeconfig location `~/.kube/config` unless provided by the `kubeconfig_path` variable

```
terraform plan -var="kubeconfig_path=$KUBECONFIG"
```
## Step 3. Apply the module.
```
terraform apply -var="kubeconfig_path=$KUBECONFIG"
```