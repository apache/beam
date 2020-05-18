---
title: "SDK Harness Configuration"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SDK Harness Configuration

Beam allows configuration of the [SDK harness](/roadmap/portability/) to
accommodate varying cluster setups.
(The options below are for Python, but much of this information should apply to the Java and Go SDKs
as well.)

- `environment_type` determines where user code will be executed.
  `environment_config` configures the environment depending on the value of `environment_type`.
  - `DOCKER` (default): User code is executed within a container started on each worker node.
    This requires docker to be installed on worker nodes.
    - `environment_config`: URL for the Docker container image. Official Docker images
    are available [here](https://hub.docker.com/u/apachebeam) and are used by default.
    Alternatively, you can build your own image by following the instructions
    [here](/documentation/runtime/environments/).
  - `PROCESS`: User code is executed by processes that are automatically started by the runner on
    each worker node.
    - `environment_config`: JSON of the form `{"os": "<OS>", "arch": "<ARCHITECTURE>",
    "command": "<process to execute>", "env":{"<Environment variables 1>": "<ENV_VAL>"} }`. All
    fields in the JSON are optional except `command`.
      - For `command`, it is recommended to use the bootloader executable, which can be built from
        source with `./gradlew :sdks:python:container:build` and copied from
        `sdks/python/container/build/target/launcher/linux_amd64/boot` to worker machines.
        Note that the Python bootloader assumes Python and the `apache_beam` module are installed
        on each worker machine.
  - `EXTERNAL`: User code will be dispatched to an external service. For example, one can start
    an external service for Python workers by running
    `docker run -p=50000:50000 apache/beam_python3.6_sdk --worker_pool`.
    - `environment_config`: Address for the external service, e.g. `localhost:50000`.
    - To access a Dockerized worker pool service from a Mac or Windows client, set the
      `BEAM_WORKER_POOL_IN_DOCKER_VM` environment variable on the client:
      `export BEAM_WORKER_POOL_IN_DOCKER_VM=1`.
  - `LOOPBACK`: User code is executed within the same process that submitted the pipeline. This
    option is useful for local testing. However, it is not suitable for a production environment,
    as it performs work on the machine the job originated from.
    - `environment_config` is not used for the `LOOPBACK` environment.
- `sdk_worker_parallelism` sets the number of SDK workers that will run on each worker node.