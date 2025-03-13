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
[terraform](https://terraform.io) to provision resources for Apache Beam tests
requiring Google Cloud resources.

# Requirements

Usage of this code requires:

- [terraform cli v1.2.0 and later](https://terraform.io)
- [Google Cloud SDK](https://cloud.google.com/sdk); `gcloud init`
  and `gcloud auth`
- Google Cloud project with billing enabled
- IntelliJ or VS Code terraform plugin (optional but **highly** recommended)

# Usage

Each folder contains all the code to achieve a category of
provisioning. For example, the [google-kubernetes-engine](google-kubernetes-engine) folder contains
all the necessary code to provision Google Cloud Kubernetes Engine cluster.

To provision resources in a chosen folder follow the conventional terraform
workflow. See https://developer.hashicorp.com/terraform/intro/core-workflow
for details.
