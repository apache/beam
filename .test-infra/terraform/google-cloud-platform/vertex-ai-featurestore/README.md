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

This module provisions a
[Vertex AI Featurestore](https://cloud.google.com/vertex-ai/docs/featurestore).

# Requirements and Usage

See [Google Cloud Platform requirements](../../../google-cloud-platform/README.md)
for details on requirements
and usage.

## 1. Initialize the terraform module

```
cd .test-infra/terraform/google-cloud-platform/vertex-ai-featurestore
terraform init
```

## 2. Create a *.tfvars file

Create a `*.tfvars` file in the same directory as this module.

```
cd .test-infra/terraform/google-cloud-platform/vertex-ai-featurestore
touch vars.tfvars
```

See [Examples](#examples) below for some example `*.tfvars` files.

## 3. Apply the terraform module.

```
cd .test-infra/terraform/google-cloud-platform/vertex-ai-featurestore
terraform apply -var-file=vars.tfvars
```

# Examples

## synthea.tfvars

This directory holds a [synthea.tfvars](synthea.tfvars) to generate an
example Vertex AI Featurestore based on data generated from
https://github.com/synthetichealth/synthea
and stored in Google Cloud FHIR Store with BigQuery streaming.
See: https://cloud.google.com/healthcare-api/docs/how-tos/fhir-bigquery-streaming
for more details.

To apply using this `*.tfvars` file:

```
cd .test-infra/terraform/google-cloud-platform/vertex-ai-featurestore
terraform apply -var-file=synthea.tfvars
```

You will be prompted for any remaining unset variables.
