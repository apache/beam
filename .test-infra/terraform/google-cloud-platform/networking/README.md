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

This folder contains Infrastructure-as-Code implemented using
[terraform](https://terraform.io) to provision resources
for Apache Beam tests requiring networking specific Google Cloud resources.

To support the [`usePublicIps=false`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/runners/dataflow/options/DataflowPipelineWorkerPoolOptions.html#setUsePublicIps-java.lang.Boolean-)
flag for Dataflow jobs, code in the folder:
- Validates the Google Cloud Virtual Private Cloud (VPC) subnetwork has
has [private Google Access](https://cloud.google.com/vpc/docs/private-google-access)
turned on
- Provisions a [Cloud NAT](https://cloud.google.com/nat/docs/overview) and
[Cloud Router](https://cloud.google.com/network-connectivity/docs/router/concepts/overview)

# Requirements

Usage of this code requires:

- [terraform cli v1.2.0 and later](https://terraform.io)
- [Google Cloud SDK](https://cloud.google.com/sdk); `gcloud init`
  and `gcloud auth`
- Google Cloud project with billing enabled
- IntelliJ or VS Code terraform plugin (optional but **highly** recommended)

# Usage

To provision resources in a this folder follow the conventional terraform
workflow. See https://developer.hashicorp.com/terraform/intro/core-workflow
for details.
