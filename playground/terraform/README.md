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

## Requirements and setup

The following items need to be setup for the Playground cluster deployment on GCP:

* [GCP account](https://cloud.google.com/) 
* [`gcloud` command-line tool](https://cloud.google.com/sdk/gcloud)
* [Terraform](https://www.terraform.io/downloads.html) tool
* [Docker](https://www.docker.com/get-started)

Authentication is required for the deployment process. `gcloud` tool can be used to log in into the GCP account:

```bash
$ gcloud auth login
```

Service account also can be used for authentication using `gcloud` cli. See more
details [here](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account).

## GCP infrastructure deployment

To deploy Playground infrastructure follow [README.md](./infrastructure/README.md) for infrastructure module.

## Playground application deployment

Playground requires building and pushing to registry using gradle before applying of Terraform scripts.

To build and push Playground Docker image to the [Artifact Registry](https://cloud.google.com/artifact-registry)
from Apache Beam repository root, execute the following commands:

```bash
$ cd /path/to/beam
$ ./gradlew playground dockerTagPush
```

To deploy Playground applications to Cloud App Engine see [README.md](./applications/README.md) from applications
module.