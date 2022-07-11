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

# Apache Beam Playground

## GitHub Actions

### Pull request run

Those runs are results of PR from the forks made by contributors. Most builds for Apache Beam fall
into this category. They are executed in the context of the "Fork", not main
Beam Code Repository which means that they have only "read" permission to all the GitHub resources
(container registry, code repository). This is necessary as the code in those PRs (including CI job
definition) might be modified by people who are not committers for the Apache Beam Code Repository.

The main purpose of those jobs is to check if PR builds cleanly, if the test run properly and if
the PR is ready to review and merge.

### Direct Push/Merge Run

Those runs are results of direct pushes done by the committers or as result of merge of a Pull Request
by the committers. Those runs execute in the context of the Apache Beam Code Repository and have also
write permission for GitHub resources (container registry, code repository).
The main purpose for the run is to check if the code after merge still holds all the assertions - like
whether it still builds, all tests are green.

This is needed because some of the conflicting changes from multiple PRs might cause build and test failures
after merge even if they do not fail in isolation.

### Workflows

#### Build And Deploy Playground Backend Application - [build_playground_backend.yml](.github/workflows/build_playground_backend.yml)

| Job                                 | Description                                                         | Pull Request Run | Direct Push/Merge Run | Requires GCP Credentials |
|-------------------------------------|---------------------------------------------------------------------|------------------|-----------------------|--------------------------|
| Check out the repo                  | GitHub Action used to check-out a repository.                       | Yes              | Yes                   | No                       |
| setup-java                          | Install Java.                                                       | Yes              | Yes                   | No                       |
| setup-go                            | Install Go.                                                         | Yes              | Yes                   | No                       |
| maven config clean                  | Clean maven settings                                                | Yes              | Yes                   | No                       |
| set up cloud sdk and its components | Setting up cloud client and its components for tests                | Yes              | Yes                   | No                       |
| playground:backend:precommit        | Pre commit  playground.                                             | Yes              | Yes                   | No                       |
| npm install                         | Install nmp package.                                                | Yes              | Yes                   | No                       |
| lint dockerfile                     | Install and  lint docker file.                                      | Yes              | Yes                   | No                       |
| setup-terraform                     | Install terraform.                                                  | Yes              | Yes                   | No                       |
| Docker Tag                          | Add tag , if it not set.                                            | Yes              | Yes                   | No                       |
| GCP account                         | Connect to Gcp.                                                     | Yes              | Yes                   | Yes                      |
| Login to Docker                     | Login to docker repository.                                         | Yes              | Yes                   | Yes                      |
| Deploy Backend                      | Build docker container,  push it  to repository,  deploy it to Gcp. | Yes              | Yes                   | Yes                      |

#### Build And Deploy Playground Frontend Application - [build_playground_frontend.yml](.github/workflows/build_playground_frontend.yml)

| Job                                    | Description                                                          | Pull Request Run | Direct Push/Merge Run | Requires GCP Credentials |
|----------------------------------------|----------------------------------------------------------------------|------------------|-----------------------|--------------------------|
| Check out the repo                     | GitHub Action used to check-out a repository.                        | Yes              | Yes                   | No                       |
| setup-java                             | Install Java.                                                        | Yes              | Yes                   | No                       |
| install flutter                        | Install flutter package.                                             | Yes              | Yes                   | No                       |
| maven config clean                     | Clean maven settings                                                 | Yes              | Yes                   | No                       |
| install npm                            | Install nmp package.                                                 | Yes              | Yes                   | No                       |
| lint dockerfile                        | Install and  lint docker file.                                       | Yes              | Yes                   | No                       |
| setup-terraform                        | Install terraform.                                                   | Yes              | Yes                   | No                       |
| Docker Tag                             | Add tag , if it not set.                                             | Yes              | Yes                   | No                       |
| GCP account                            | Connect to Gcp.                                                      | Yes              | Yes                   | Yes                      |
| Login to Docker                        | Login to docker repository.                                          | Yes              | Yes                   | Yes                      |
| Deploy Frontend                        | Config and build, push docker container to repository, deploy to Gcp.| Yes              | Yes                   | Yes                      |

#### Collect And Deploy Playground Examples - [playground_deploy_examples.yml](.github/workflows/playground_deploy_examples.yml)

| Job                                    | Description                                                          | Pull Request Run | Direct Push/Merge Run | Requires GCP Credentials |
|----------------------------------------|----------------------------------------------------------------------|------------------|-----------------------|--------------------------|
| Check out the repo                     | GitHub Action used to check-out a repository.                        | Yes              | Yes                   | No                       |
| setup-python                           | Install Python.                                                      | Yes              | Yes                   | No                       |
| setup-java                             | Install Java.                                                        | Yes              | Yes                   | No                       |
| Install kubectl                        | Install tool  kubectl for kubernetes cloud                           | Yes              | Yes                   | No                       |
| Install helm                           | Install Help plugin                                                  | Yes              | Yes                   | No                       |
| Set up Cloud SDK                       | Install GCP Cloud client                                             | Yes              | Yes                   | No                       |
| install deps                           | Add packages:                                                        | Yes              | Yes                   | No                       |
|                                        |      grpcio-tools,grpcio,mock,protobuf, pytest,pytest-mock,          |                  |                       |                          |
|                                        |      PyYAML,google-cloud-storage,tqdm                                |                  |                       |                          |
| maven config clean                     | Clean maven settings                                                 | Yes              | Yes                   | No                       |
| GCP account                            | Connect to Gcp.                                                      | Yes              | Yes                   | Yes                      |
| Docker Tag                             | Add tag , if it not set.                                             | Yes              | Yes                   | No                       |
| Get K8s Config                         | Get config for kubectl                                               | Yes              | Yes                   | Yes                      |
| Login to Docker                        | Login to docker repository.                                          | Yes              | Yes                   | Yes                      |
| Build And Push Apps                    | Build and push docker containers for app                             | Yes              | Yes                   | Yes                      |
| Install helm chart                     | Deploy app to  kubernetes cloud                                      | Yes              | Yes                   | Yes                      |
| Run Python Examples CI                 | Prepare examples for Python                                          | Yes              | Yes                   | Yes                      |
| Run Python Examples CD                 | Execute examples for Python                                          | Yes              | Yes                   | Yes                      |
| Run Go Examples CI                     | Prepare examples for Go                                              | Yes              | Yes                   | Yes                      |
| Run Go Examples CD                     | Execute examples for Go                                              | Yes              | Yes                   | Yes                      |
| Run Java Examples CI                   | Prepare examples for Java                                            | Yes              | Yes                   | Yes                      |
| Run Java Examples CD                   | Execute examples for Java                                            | Yes              | Yes                   | Yes                      |
| Delete Helm Chart                      | Drop kubernetes cloud                                                | Yes              | Yes                   | Yes                      |

#### Secrets for action

 - GCP_PLAYGROUND_REGION - gcp main region location cloud (default:us-central1)
 - GCP_PLAYGROUND_SA_EMAIL - gcp service account id (default:playground-deploy@apache-beam-testing.iam.gserviceaccount.com)
 - GCP_PLAYGROUND_PROJECT_ID - gcp project id (default:apache-beam-testing)
 - PLAYGROUND_REGISTRY_NAME - gcp docker registry address (default:us-central1-docker.pkg.dev)
 - GCP_PLAYGROUND_SA_KEY - gcp private key file, it export from service account and encode to Base64


