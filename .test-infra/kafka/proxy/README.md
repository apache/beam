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

# Kafka proxy

Provisions a private IP bastion host on Google Cloud for use as a proxy to a private IP Kafka instance.

# Prerequisites

- Kafka cluster (See [.test-infra/kafka](..) for available solutions.)

# Usage

## Acquire bootstrap server hosts

One of the variables requires a mapping of bootstrap server hosts to the desired proxy exposed port. See
the variable description for `bootstrap_endpoint_mapping` found in the [variables.tf](variables.tf) file.

## Apply module

Follows typical terraform workflow without the use of a
[backend](https://developer.hashicorp.com/terraform/language/settings/backends/configuration).

```
DIR=.test-infra/kafka/proxy
terraform -chdir=$DIR init
```

```
terraform -chdir=$DIR apply -var-file=common.tfvars -var-file=name_of_your_specific.tfvars
```

## Invoke gcloud ssh tunneling command

Successful application of the module will output the specific gcloud command needed to tunnel the kafka traffic
to your local machine. An example of such output would look similar to:

```
gcloud compute ssh yourinstance --tunnel-through-iap --project=project --zone=zone --ssh-flag="-4 -L9093:localhost:9093" --ssh-flag="-4 -L9092:localhost:9092" --ssh-flag="-4 -L9094:localhost:9094"
```
