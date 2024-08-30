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

This directory holds all the terraform modules for setting up the Google Cloud
(GCP) resources necessary to executing [:beam-test-infra-pipelines](../)
pipelines using the Dataflow runner.

# Code organization

Folders are named according to recommended order of execution. For example,
[01.setup](01.setup) is intended to be used prior to [02.network](02.network).

# Common Terraform Modules

The following terraform modules apply to all executable
[:beam-test-infra-pipelines](../) pipelines.

| Path                     | Purpose           |
|--------------------------|-------------------|
| [01.setup](01.setup)     | Setup GCP project |
| [02.network](02.network) | Provision network |

# Specific Terraform Modules

The following modules apply to specific pipelines. When creating a new
executable pipeline and its supported terraform, please consider updating this
documentation.

## org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery

The following modules provision resources related to
[`org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery`](../src/main/java/org/apache/beam/testinfra/pipelines/ReadDataflowApiWriteBigQuery.java).

| Path                                                                 | Purpose                                                                  | Required/Optional |
|----------------------------------------------------------------------|--------------------------------------------------------------------------|-------------------|
| [03.io/dataflow-to-bigquery](03.io/dataflow-to-bigquery)             | Provisions resources to read from the Dataflow API and write to BigQuery | required          |
| [04.template/dataflow-to-bigquery](04.template/dataflow-to-bigquery) | Builds a Dataflow Flex Template that executes the pipelines              | optional          |

Therefore, to run
[`org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery`](../src/main/java/org/apache/beam/testinfra/pipelines/ReadDataflowApiWriteBigQuery.java),
apply the following recommended order of terraform modules. See their respective
READMEs for more details.

1. [01.setup](01.setup)
2. [02.network](02.network)
3. [03.io/dataflow-to-bigquery](03.io/dataflow-to-bigquery)
4. [04.template/dataflow-to-bigquery](04.template/dataflow-to-bigquery) (_if you want to use Dataflow Flex Templates_)
