<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

## Helper Functions

### GCP Cloud Functions
For the local implementation of the following three functions is required to create an `.env` file. If you'd like to reproduce this code in a GCP Cloud Functions, you will need to replace the variables in the `Variables` section either as `Runtime environment variables` or `Secrets`.

```
APP_INSTALLATION_ID=
APP_ID=
CLIENT_ID=
PEM_KEY=
CLIENT_NAME=
ORG=
PROJECT_ID=
```

#### generateToken
Function implemented to retrieve the GitHub App token for register a self-hosted runner. It's used in the [entrypoint](../self-hosted-linux/docker/entrypoint.sh) of the Dockerfile.
#####  monitorRunnersStatus
Function implemented to get the status of the self-hosted runners. It's used in the [monitor_self_hosted_runners](../../workflows/monitor_self_hosted_runners.yml) workflow.
#### removeOfflineRunners
Function implemented to delete the unused self-hosted runners. Please refer to this [README](../self-hosted-linux/README.md) for more details.