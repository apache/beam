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

The Beam Playground is a web application to run Beam code snippets in a modern browser. This directory holds code to
build, test, and deploy the frontend and backend services.

- [Setup development prerequisites](#setup-development-prerequisites)
- [Common tasks](#common-tasks)
- [Run Beam Playground locally](#run-beam-playground-locally)
  - [Configure frontend to use local backend](#configure-frontend-to-use-local-backend)
  - [Run local deployment using Gradle task](#run-local-deployment-using-gradle-task)
  - [Deploy examples](#deploy-examples)
- [How to add your own example](#how-to-add-your-own-example)
- [Deployment guide](#deployment-guide)
  - [Manual deployment](#manual-deployment)
  - [Manual CloudBuild setup](#manual-cloudbuild-setup)
- [Project structure](#project-structure)
- [Contribution guide](#contribution-guide)

# Setup development prerequisites
> ***Google Cloud Shell note***: Google Cloud Shell already has most of the prerequisites installed. Only the following has to be installed manually:
> - Flutter - run `flutter precache`
> - Go protobuf dependencies
> - Dart protobuf dependencies
> - buf
> - sbt

1. Install Go 1.23+

    **Ubuntu 22.04 and newer:**
    ```shell
    sudo apt install golang
    ```

    **Other Linux variants:** Follow manual at https://go.dev/doc/install
1. Install [flutter](https://docs.flutter.dev/get-started/install/linux)

    **Ubuntu 22.04 or newer:**
    ```shell
    sudo apt install flutter
    ```

    **Other Linux variants:** Follow manual at https://flutter.dev/docs/get-started/install/linux

1. Install [protoc](https://grpc.io/docs/protoc-installation/)

    **Ubuntu 22.04 or newer/Debian 11 or newer:**
    ```shell
    sudo apt install protobuf-compiler
    ```
    **Other Linux variants:** Follow manual at https://grpc.io/docs/protoc-installation/

1. Install Go protobuf dependencies: [Go gRPC Quickstart](https://grpc.io/docs/languages/go/quickstart/)
    > Do not forget to update your `PATH` environment variable
1. Install Dart protobuf dependencies: [Dart gRPC Quickstart](https://grpc.io/docs/languages/dart/quickstart/)
    > Do not forget to update your `PATH` environment variable
1. Install npm
    **Ubuntu 22.04 and newer/Debian 11 and newer:**
    ```shell
    sudo apt install npm
    ```
    **Other Linux variants:** Follow manual at https://docs.npmjs.com/downloading-and-installing-node-js-and-npm
1. Install [buf](https://docs.buf.build/installation)
    ```shell
    npm install -g @bufbuild/buf
    ```
1. Install Docker
    **Ubuntu 22.04 and newer/Debian 11 and newer:**
    ```shell
    sudo apt install docker.io
    ```
    **Other Linux variants:** Follow manual at https://docs.docker.com/engine/install/

    To verify your docker installation, run:
    ```shell
    docker ps
    ```
    It should finish without any errors. If you get a permission denied error, you need to add your user to the docker group:
    ```shell
    sudo usermod -aG docker $USER
    ```
    Then, log out and log back in to apply the changes.
1. Install Docker Compose
    **Ubuntu 22.04 and newer/Debian 11 and newer:**
    ```shell
    sudo apt install docker-compose
    ```
    **Other Linux variants:** Follow manual at https://docs.docker.com/compose/install/
1. Install gcloud CLI by following the [manual](https://cloud.google.com/sdk/docs/install) for your system
1. Install Cloud Datastore Emulator
    ```shell
    gcloud components install cloud-datastore-emulator
    ```
    or, if you have install gcloud CLI using APT:
    ```shell
    sudo apt install google-cloud-cli-datastore-emulator
    ```
1. Install `sbt` following instruction at https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html
    > **Optional**: Run `sbt` comamnd once after installation to let it cache its dependencies
1. Make sure that you have the full installation of Python 3.8 or newer. On Debian or Ubuntu it can be installed using `sudo apt install python3-full`.

# Common tasks
To get an overview of common tasks, see [TASKS.md](TASKS.md)

# Run Beam Playground locally
## Configure frontend to use local backend
> **Note:** Follow this step only if you want to have a local deployment of Playground. Skip this step entirely if you want to deploy Playground to Google Cloud.

Uncommend lines after `// Uncomment the following lines to use local backend.` in [frontend/playground_components/lib/src/constants/backend_urls.dart](/playground/frontend/playground_components/lib/src/constants/backend_urls.dart)

## Run local deployment using Gradle task
> For more information read the corresponding section in [TASKS.md](./TASKS.md#router-runners-and-frontend)

Run the deployment script:
```shell
./gradlew :playground:dockerComposeLocalUp
```

To shut down the playground, run:
```shell
./gradlew :playground:dockerComposeLocalDown
```

## Deploy examples
1. Go to `/playground/infrastucture` directory
    ```shell
    cd playground/infrastucture
    ```
1. Setup python venv
    ```shell
    python3 -m venv venv
    ```
1. Activate venv
    ```shell
    source venv/bin/activate
    ```
1. Install dependencies
    ```shell
    pip install -r requirements.txt
    ```
1. Setup the environment variables
    ```shell
    export BEAM_ROOT_DIR=$(realpath ../../)
    export SDK_CONFIG="../../playground/sdks.yaml"
    export BEAM_EXAMPLE_CATEGORIES="../categories.yaml"
    export BEAM_USE_WEBGRPC=yes
    export BEAM_CONCURRENCY=4
    export DATASTORE_EMULATOR_HOST=localhost:8081
    ```

1. Run the `ci_cd.py` script

    ```shell
    export SERVER_ADDRESS=<runner_address> # see the note below
    python ci_cd.py --step CD \
                    --sdk <SDK> \ # SDK_GO, SDK_JAVA, SDK_PYTHON, SDK_SCIO
                    --namespace Playground \
                    --datastore-project test \
                    --origin PG_EXAMPLES \
                    --subdirs $BEAM_ROOT_DIR/sdks $BEAM_ROOT_DIR/examples $BEAM_ROOT_DIR/learning/katas # For SCIO examples see note below
    ```
    > **Note:** The `SERVER_ADDRESS` variable should be set to the address of the runner server for the particular SDK. For the local deployment the default values are:
    > | SDK | Address |
    > | --- | --- |
    > | Go | `localhost:8084` |
    > | Java | `localhost:8086` |
    > | Python | `localhost:8088` |
    > | SCIO | `localhost:8090` |

    > **Note:** SCIO examples are not committed to the Beam tree. You will need to use [`fetch_scala_examples.py`](infrastructure/fetch_scala_examples.py) script to download them to some directory and then use `--subdirs` option to point to that directory. For example:
    > ```shell
    > python fetch_scala_examples.py --output-dir /tmp/scio-examples
    > python ci_cd.py --step CD \
    >                 --sdk SDK_SCIO \
    >                 --namespace Playground \
    >                 --datastore-project test \
    >                 --origin PG_EXAMPLES \
    >                 --subdirs /tmp/scio-examples
    > ```
    > See [this section](TASKS.md##obtaining-scio-examples) for more information.

# How to add your own example
Please refer to [this document](load_your_code.md).

# Deployment guide
## Manual deployment
See deployment guide at [terraform/README.md](/playground/terraform/README.md)
## Manual CloudBuild setup
To set up CloudBuild triggers manually please refer to [this guide](/playground/terraform/infrastructure/cloudbuild-manual-setup/README.md)

# Project structure

Several directories in this repository are used for the Beam Playground project. The list of the directories used can be seen below.

| Directory | Purpose |
|-----------|---------|
| [`/examples`](/examples) | Contains code of the examples for Java SDK. The examples are getting loaded into the main catalog. |
| [`/learning/beamdoc`](/learning/beamdoc) | Contains code of the examples which should not be available in the main Playground catalog. |
| [`/learning/katas`](/learning/katas) | Containes small code examples, loaded into main catalog.
| [`/playground`](/playground) | Root of Playground sources. |
| [`/playground/api`](/playground/api) | Protobuf definitions for the Playground gRPC API. |
| [`/playground/backend`](/playground/backend) | Root of Playground backend sources. See [this document](/playground/backend/CONTRIBUTE.md) for detailed description. |
| [`/playground/frontend`](/playground/frontend) | Root of Playground frontend sources. See [this document](/playground/frontend/CONTRIBUTE.md) for detailed description. |
| [`/playground/infrastructure`](/playground/infrastructure) | Scripts used in Playground CI/CD pipelines for verifying and uploading examples. |
| [`/playground/kafka-emulator`](/playground/kafka-emulator) | Sources of a Kafka emulator used for demonstrating KafkaIO examples. |
| [`/playground/terraform`](/playground/terraform) | Terraform configuration files for Playground deployment to Google Cloud Platform. |
| [`/sdks`](/sdks) | Source of the BEAM SDKs. Used by Playground as a source of examples for Go and Python SDKs. |

# Contribution guide
- Backend: see [backend/README.md](/playground/backend/README.md) and [backend/CONTRIBUTE.md](/playground/backend/CONTRIBUTE.md)
- Frontend: see [frontend/README.md](/playground/frontend/README.md) and [frontend/CONTRIBUTE.md](/playground/frontend/CONTRIBUTE.md)
