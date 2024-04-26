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

This module provisions the [strimzi operator](https://github.com/strimzi/strimzi-kafka-operator/tree/main/helm-charts/helm3/strimzi-kafka-operator).

# Requirements

- [terraform](https://terraform.io)
- Connection to a kubernetes cluster (See: [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/google-kubernetes-engine](../../terraform/google-cloud-platform/google-kubernetes-engine) in this repository)
- [kubectl](https://kubernetes.io/docs/reference/kubectl/) cli

# Usage

Simply follow standard terraform workflow to apply this module using the existing
[.test-infra/kafka/strimzi/common.tfvars](common.tfvars) for conventional defaults.

Where:

```
DIR=.test-infra/kafka/strimzi/01-strimzui-operator
VARS=common.tfvars # without the directory
```

Run:

```
terraform -chdir=$DIR init
terraform -chdir=$DIR apply -var-file=$VARS
```

This will deploy the operator. For actual strimzi kafka cluster please check out [.test-infra/kafka/02-kafka-persistent](02-kafka-persistent/README.md)