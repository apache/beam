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

# Playground

The Beam Playground is a web application to run Beam code snippets in a modern browser. This directory holds code to
build, test, and deploy the frontend and backend services.

# Development Requirements

The following requirements are needed for development, testing, and deploying.

- [go 1.18+](https://golang.org)
- [flutter](https://flutter.dev/)
- Go protobuf dependencies (See [Go gRPC Quickstart](https://grpc.io/docs/languages/go/quickstart/))
- Dart protobuf dependencies (See [Dart gRPC Quickstart](https://grpc.io/docs/languages/dart/))
- [buf](https://docs.buf.build/installation)
- [Docker](https://docs.docker.com/desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- [gcloud Beta Commands](https://cloud.google.com/sdk/gcloud/reference/components/install)
- [Cloud Datastore Emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator)
- [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html)

### Google Cloud Shell Prerequisites Installation
Google Cloud Shell already has most of the prerequisites installed. Only few tools need to be installed separately

#### Flutter
```shell
git config --global --add safe.directory /google/flutter
flutter doctor
```

#### Protobuf
```shell
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
dart pub global activate protoc_plugin
npm install -g @bufbuild/buf
```

#### sbt
```shell
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt
```

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

# Deployment

See [terraform](./terraform/README.md) for details on how to build and deploy
the application and its dependent infrastructure.

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
