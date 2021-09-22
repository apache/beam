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
# How Self-hosted runners work on GKE

## Docker image
The Dockerfile contained at the root of this folder downloads the runner application.
This runner application is the one in charge of executing the GitHub Actions Workflows.
Once the runner is downloaded, the entrypoint.sh script registers its containing Docker image as a self-hosted runner.

```sh
./config.sh \
    --name $(hostname) \
    --token ${RUNNER_TOKEN} \
    --url https://github.com/${GITHUB_OWNER}/${GITHUB_REPOSITORY} \
    --work _work \
    --unattended \
    --replace
```
The RUNNER_TOKEN is a GitHub Personal Token previously configured in GitHub under the Actions section.

## Kubernetes configuration changes.
Since there's already a GKE configuration for the execution of the Jenkins jobs, we can add extra configs to it.
```sh
.test-infra/metrics/build.gradle
.test-infra/metrics/build_and_publish_containers.sh
.test-infra/metrics/docker-compose.yml
.test-infra/metrics/kubernetes/beamgrafana-deploy.yaml
```
At this point, only the files above appear to need changes.
This can of course not be the case and maybe further files need to be modified.

More detailed instructions can be found [here](https://vitobotta.com/2020/09/29/self-hosted-github-actions-runners-in-kubernetes/)
