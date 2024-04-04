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

This module will provision a utility cluster which is used to run infrastructure needed for tests running in beam CI

# Deployment

# Step 1. Initialize and apply the terraform module.

Make sure you have set the prefix in terraform block in the provider.tf file. Change it if trying to deploy another cluster.

For initialization make sure you use the `beam-infra-terraform-state`
```
terraform init -backend-config="bucket=beam-infra-terraform-state"
```
## Step 2. Plan the module. Make sure that terraform is not replacing or deleting resrouces unless expected:
```
terraform plan
```
## Step 3. Apply the module.
```
terraform apply 
```