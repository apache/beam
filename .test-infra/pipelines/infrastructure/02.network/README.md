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

This directory provisions Google Cloud project networking for Dataflow usage.

# List of all provision GCP resources

The following table lists all provisioned resources and their rationale.

| resource       | reason                                      |
|----------------|---------------------------------------------|
| Network        | Run workload in its isolated GCP VPC        |
| Subnetwork     | Worker needs at least one subnetwork        |
| Firewall Rules | Limit traffic to Worker service account VMS |

# Usage

Follow terraform workflow convention to apply this module. It assumes the
working directory is at
[.test-infra/pipelines](../..)

Notice the `-var-file` flag referencing [common.tfvars](common.tfvars) that
provides opinionated variable defaults.

For example:

```
DIR=infrastructure/02.network
terraform -chdir=$DIR init
terraform -chdir=$DIR apply -var-file=common.tfvars -var=project=$(gcloud config get-value project)
```
