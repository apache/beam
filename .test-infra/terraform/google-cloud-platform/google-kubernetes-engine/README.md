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

This module provisions a private Google Kubernetes Engine cluster.

# Requirements and Usage

See [Google Cloud Platform requirements](../../google-cloud-platform/README.md)
for details on requirements
and usage.

## 1. Create vars.tfvars

Create a `vars.tfvars` file
in [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine](.).
Edit with your IDE terraform plugin installed and it will autocomplete the
variable names.

## 2. Initialize and apply the terraform module.

```
terraform init
terraform plan -var-file=vars.tfvars
terraform apply -var-file=vars.tfvars
```

# Special Instructions

This module also provisions a bastion host needed to connect to the private
cluster. To connect to the kubernetes
cluster, do so through the bastion host by following directions starting at
[Connect to your cluster from the remote client](https://cloud.google.com/kubernetes-engine/docs/tutorials/private-cluster-bastion#connect).

To find the bastion host, run:

```
gcloud compute instances list --filter=name:bastion
```
