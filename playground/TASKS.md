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

- [Available Gradle Tasks](#available-gradle-tasks)
  - [Perform overall pre-commit checks](#perform-overall-pre-commit-checks)
  - [To see available gradle tasks for playground:](#to-see-available-gradle-tasks-for-playground)
  - [Re-generate protobuf](#re-generate-protobuf)
  - [Run local environment using docker compose](#run-local-environment-using-docker-compose)
    - [Router only](#router-only)
    - [Router, runners, and frontend](#router-runners-and-frontend)
  - [Removing old snippets](#removing-old-snippets)
  - [Removing a specific snippet](#removing-a-specific-snippet)
  - [Run playground tests without cache](#run-playground-tests-without-cache)
- [Updating dependencies](#updating-dependencies)
  - [Referenced Beam SDK update](#referenced-beam-sdk-update)
  - [Update Go version](#update-go-version)
  - [Update Python version](#update-python-version)
  - [Update Java version](#update-java-version)
  - [Update Flutter version](#update-flutter-version)
- [Deployment](#deployment)
- [Obtaining SCIO examples](#obtaining-scio-examples)
- [Manual Example deployment](#manual-example-deployment)
  - [Run example deployment script](#run-example-deployment-script)


# Available Gradle Tasks

## Perform overall pre-commit checks

```
cd beam
./gradlew playgroundPrecommit
```

## To see available gradle tasks for playground:

```
cd beam
./gradlew playground:tasks
```

## Re-generate protobuf

```
cd beam
./gradlew playground:generateProto
```

## Run local environment using docker compose

### Router only

Start:

```bash
cd beam
./gradlew playground:backend:containers:router:dockerComposeLocalUp
```

Stop:

```bash
cd beam
./gradlew playground:backend:containers:router:dockerComposeLocalDown
```

### Router, runners, and frontend

1. Edit `/playground/frontend/playground_components/lib/src/constants/backend_urls.dart`
to override backend URLs with yours found in `/playground/docker-compose.local.yaml`.
2. To start, run:

```bash
cd beam
./gradlew playground:dockerComposeLocalUp
```

3. To stop, run:

```bash
cd beam
./gradlew playground:dockerComposeLocalDown
```

This way of running may not work in all environments because it is not maintained.
It is used occasionally by the Frontend team to test complex tasks against
a not-yet-deployed backend.
The full start may take ~30 minutes and is demanding, so you should likely enable
only one backend runner for the SDK you need.

If you do not need particular runners, comment out:
1. Dependencies on them in `/playground/build.gradle.kts` in `dockerComposeLocalUp` task.
2. Their Docker image configurations in `/playground/docker-compose.local.yaml`.

See also [Backend Lookup](frontend/README.md#backend-lookup) in the Frontend.

## Removing old snippets

Run the method to remove unused code snippets from the Cloud Datastore. Unused snippets are snippets that are out of date. If the last visited date property less or equals than the current date minus dayDiff parameter then a snippet is out of date

```
cd beam
./gradlew playground:backend:removeUnusedSnippet -DdayDiff={int} -DprojectId={string} -Dnamespace={datastore namespace}
```

## Removing a specific snippet

Run the method to remove a specific code snippet from the Cloud Datastore.

```
cd beam
./gradlew playground:backend:removeSnippet -DsnippetId={string} -DprojectId={string} -Dnamespace={datastore namespace}
```

## Run playground tests without cache

```
cd beam
 ./gradlew playground:backend:testWithoutCache
```

# Updating dependencies
## Referenced Beam SDK update
1. Update default `BEAM_VERSION` values in Java contianer [Dockerfile](/playground/backend/containers/java/Dockerfile)
2. Update `default_beam_version` in [Java container `build.gradle`](/playground/backend/containers/java/build.gradle)
3. Update `default_beam_version` in [Python container `build.gradle`](/playground/backend/containers/python/build.gradle)
4. Update SDK version in CI GitHub workflow:
    - [playground_examples_ci_reusable.yml](/.github/workflows/playground_examples_ci_reusable.yml)
5. Update `-Psdk-tag=` in ["Deploy Playground to Kubernetes" section of the deployment guide](/playground/terraform/README.md#deploy-playground-to-kubernetes)
6. Update `_BEAM_VERSION` in
    - [playground_ci_stable.yaml](/playground/infrastructure/cloudbuild/playground_ci_stable.yaml)
    - [playground_cd_stable.yaml](/playground/infrastructure/cloudbuild/playground_cd_stable.yaml)

## Update Go version

1. Update Go version in [`go.mod`](backend/go.mod)
2. Run `go mod download` and `go mod tidy` in [`backend`](backend) directory
3. Update `BASE_IMAGE` parameter in [router Dockerfile](backend/containers/router/Dockerfile) and [`build.gradle`](backend/containers/router/build.gradle)
4. Update `GO_BASE_IMAGE` parameter in [Python Dockerfile](backend/containers/python/Dockerfile) and [`build.gradle`](backend/containers/python/build.gradle)
5. Update `GO_BASE_IMAGE` parameter in [Java Dockerfile](backend/containers/java/Dockerfile) and [`build.gradle`](backend/containers/java/build.gradle)
6. Update `GO_BASE_IMAGE` parameter in [SCIO Dockerfile](backend/containers/scio/Dockerfile) and [`build.gradle`](backend/containers/scio/build.gradle)
7. Update `BASE_IMAGE` for `buildArgs` argument in [Go runner `build.gradle`](backend/containers/go/build.gradle)

## Update Python version

Update `BASE_IMAGE` in [Python Dockerfile](backend/containers/python/Dockerfile)

## Update Java version

1. Update base images version in `FROM maven:3.8.6-openjdk-11 as dep` and `FROM apache/beam_java11_sdk:$BEAM_VERSION` lines in [Java Dockerfile](backend/containers/java/Dockerfile)
2. Update `BASE_IMAGE` in [SCIO Dockerfile](backend/containers/scio/Dockerfile) and [`build.gradle`](backend/containers/scio/build.gradle)

## Update Flutter version

Update the version in the following locations:

### Project files that declare minimal required versions as we desire, one per Flutter project

- [frontend/playground_components/pubspec.yaml](frontend/playground_components/pubspec.yaml), the line with `flutter: '>=x.x.x'`.
- [frontend/playground_components_dev/pubspec.yaml](frontend/playground_components_dev/pubspec.yaml), the line with `flutter: '>=x.x.x'`.
- [frontend/pubspec.yaml](frontend/pubspec.yaml), the line with `flutter: '>=x.x.x'`.
- In Tour of Beam: [../learning/tour-of-beam/frontend/pubspec.yaml](../learning/tour-of-beam/frontend/pubspec.yaml), the line with `flutter: '>=x.x.x'`.

### Generated project files with computed minimal versions given the package requirements, one per runnable Flutter app

Run `dart pub get` in all of these directories for these files to be updated.
Verify and merge the changes.

- Playground: [frontend/pubspec.lock](frontend/pubspec.lock), the line with `flutter: ">=x.x.x"`.
- Tour of Beam: [../learning/tour-of-beam/frontend/pubspec.lock](../learning/tour-of-beam/frontend/pubspec.lock), the line with `flutter: ">=x.x.x"`.

### CI

- [frontend/Dockerfile](frontend/Dockerfile), the line with `ARG FLUTTER_VERSION=x.x.x`.
- [/.github/workflows/build_playground_frontend.yml](../.github/workflows/build_playground_frontend.yml), the line with `FLUTTER_VERSION: x.x.x`.
- [/.github/workflows/playground_frontend_test.yml](../.github/workflows/playground_frontend_test.yml), the line with `FLUTTER_VERSION: x.x.x`.
- [/.github/workflows/tour_of_beam_frontend_test.yml](../.github/workflows/tour_of_beam_frontend_test.yml), the line with `FLUTTER_VERSION: x.x.x`.

# Deployment

See [this guide](./terraform/README.md) for details on how to build and deploy
the application and its dependent infrastructure.

# Obtaining SCIO examples

SCIO examples are based  on examples found in the upstream SCIO [repository](https://github.com/spotify/scio). To obtain the examples, use [`fetch_scala_examples.py`](infrastructure/fetch_scala_examples.py) script:

```shell
python fetch_scala_examples.py --output_dir <output_dir>
```
this will download all supported SCIO examples into `<output_dir>`.

To add a new example, add it to the list of examples in the script [`fetch_scala_examples.py`](infrastructure/fetch_scala_examples.py). You will need to provide the path to the example in the `scio` git repository, the name of the example, description, run options, categories and tags.

# Manual Example deployment

The following requirements are needed for deploying examples manually:

1. GCP project with deployed Playground backend
2. Python (3.9.x)
3. Login into GCP (gcloud default login or using service account key)

## Run example deployment script
Example deployment scripts uses following environment variables:

- GOOGLE_CLOUD_PROJECT    - GCP project id where Playground backend is deployed
- BEAM_ROOT_DIR           - root folder to search for playground examples
- SDK_CONFIG              - location of sdk and default example configuration file
- BEAM_EXAMPLE_CATEGORIES - location of example category configuration file
- BEAM_USE_WEBGRPC        - use grpc-Web instead of grpc (default)
- GRPC_TIMEOUT            - timeout for grpc calls (defaults to 10 sec)
- BEAM_CONCURRENCY        - number of eaxmples to run in parallel (defaults to 10)
- SERVER_ADDRESS          - address of the backend runnner service for a particular SDK

```
usage: ci_cd.py [-h] --step {CI,CD} [--namespace NAMESPACE] --datastore-project DATASTORE_PROJECT --sdk {SDK_JAVA,SDK_GO,SDK_PYTHON,SDK_SCIO} --origin {PG_EXAMPLES,TB_EXAMPLES} --subdirs SUBDIRS [SUBDIRS ...]

CI/CD Steps for Playground objects

optional arguments:
  -h, --help            show this help message and exit
  --step {CI,CD}        CI step to verify all beam examples/tests/katas. CD step to save all beam examples/tests/katas and their outputs on the GCD
  --namespace NAMESPACE
                        Datastore namespace to use when saving data (default: Playground)
  --datastore-project DATASTORE_PROJECT
                        Datastore project to use when saving data
  --sdk {SDK_JAVA,SDK_GO,SDK_PYTHON,SDK_SCIO}
                        Supported SDKs
  --origin {PG_EXAMPLES,TB_EXAMPLES}
                        ORIGIN field of pg_examples/pg_snippets
  --subdirs SUBDIRS [SUBDIRS ...]
                        limit sub directories to walk through, relative to BEAM_ROOT_DIR
```

Helper script to deploy examples for all supported sdk's:

```shell
cd playground/infrastructure

export BEAM_ROOT_DIR="../../"
export SDK_CONFIG="../../playground/sdks.yaml"
export BEAM_EXAMPLE_CATEGORIES="../categories.yaml"
export BEAM_USE_WEBGRPC=yes
export BEAM_CONCURRENCY=4
export PLAYGROUND_DNS_NAME="your registered dns name for Playground"

for sdk in go java python scio; do
    export SDK=$sdk &&
    export SERVER_ADDRESS=https://${SDK}.$PLAYGROUND_DNS_NAME &&

    python3 ci_cd.py --datastore-project $GOOGLE_CLOUD_PROJECT \
                     --step CD --sdk SDK_${SDK^^} \
                     --origin PG_EXAMPLES \
                     --subdirs ./learning/katas ./examples ./sdks
done
```
