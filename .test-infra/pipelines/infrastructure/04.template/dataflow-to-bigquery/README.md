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

This directory holds terraform code to build the
[org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery](../../../src/main/java/org/apache/beam/testinfra/pipelines/ReadDataflowApiWriteBigQuery.java)
pipeline for use as a [Dataflow Flex Template](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates).

# Why terraform?

As of this README's writing, there is no resource block for the Google Cloud
terraform provider to provision Dataflow Templates. Therefore, this solution
makes use of the
[null_resource](https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource)
along with the
[local-exec](https://developer.hashicorp.com/terraform/language/resources/provisioners/local-exec)
provisioner.

The benefit of using terraform is that it provides clean resource lookups,
within its workflow, known to make submitting the Dataflow job
cumbersome through other means, such as through a gradle command, bash script,
etc.

# Usage

Follow terraform workflow convention to apply this module. It assumes the
working directory is at
[.test-infra/pipelines](../../..)

This module does not use a state backend.

Notice the `-var-file` flag referencing [common.tfvars](common.tfvars) that
provides opinionated variable defaults.

For `apache-beam-testing`:

```
DIR=infrastructure/04.template/dataflow-to-bigquery
terraform -chdir=$DIR init
terraform -chdir=$DIR apply -var-file=common.tfvars -var-file=apache-beam-testing.tfvars
```

or for your own Google Cloud project:

```
DIR=infrastructure/04.template/dataflow-to-bigquery
terraform -chdir=$DIR init
terraform -chdir=$DIR apply -var-file=common.tfvars
```
