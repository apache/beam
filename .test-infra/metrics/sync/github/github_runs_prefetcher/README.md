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
# Github Workflows Prefetcher
Github Workflows Prefetcher is a script used to fetch workflow run data and store it in CloudSQL for further use.
It is used by Grafana (http://metrics.beam.apache.org) to display status dashboards and for alerting which is further used for failing test issue creation.

## Setup

Initialize the terraform state with
```
terraform init -backend-config="bucket=beam-arc-state"
```


Then execute `terraform apply`