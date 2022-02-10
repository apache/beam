---
title: "Container environments"
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

# Container environments

The Beam SDK runtime environment can be [containerized](https://www.docker.com/resources/what-container) with [Docker](https://www.docker.com/) to isolate it from other runtime systems. To learn more about the container environment, read the Beam [SDK Harness container contract](https://s.apache.org/beam-fn-api-container-contract).

Prebuilt SDK container images are released per supported language during Beam releases and pushed to [Docker Hub](https://hub.docker.com/search?q=apache%2Fbeam&type=image).

## Custom containers

You may want to customize container images for many reasons, including:

* Pre-installing additional dependencies
* Launching third-party software in the worker environment
* Further customizing the execution environment

 This guide describes how to create and use customized containers for the Beam SDK.

### Prerequisites

* This guide requires building images using Docker. [Install Docker locally](https://docs.docker.com/get-docker/). Some CI/CD platforms like [Google Cloud Build](https://cloud.google.com/cloud-build/docs/building/build-containers) also provide the ability to build images using Docker.
* For remote execution engines/runners, have a container registry to host your custom container image. Options include [Docker Hub](https://hub.docker.com/) or a "self-hosted" repository, including cloud-specific container registries like [Google Container Registry](https://cloud.google.com/container-registry) (GCR) or [Amazon Elastic Container Registry](https://aws.amazon.com/ecr/) (ECR). Make sure your registry can be accessed by your execution engine or runner.

>  **NOTE**: On Nov 20, 2020, Docker Hub put [rate limits](https://www.docker.com/increase-rate-limits) into effect for anonymous and free authenticated use, which may impact larger pipelines that pull containers several times.

For optimal user experience, we also recommend you use the latest released version of Beam.

### Building and pushing custom containers

Beam [SDK container images](https://hub.docker.com/search?q=apache%2Fbeam&type=image) are built from Dockerfiles checked into the [Github](https://github.com/apache/beam) repository and published to Docker Hub for every release. You can build customized containers in one of two ways:

1. **[Writing a new](#writing-new-dockerfiles) Dockerfile based on a released container image**. This is sufficient for simple additions to the image, such as adding artifacts or environment variables.
2. **[Modifying](#modifying-dockerfiles) a source Dockerfile in [Beam](https://github.com/apache/beam)**. This method requires building from Beam source but allows for greater customization of the container (including replacement of artifacts or base OS/language versions).

#### Writing a new Dockerfile based on an existing published container image {#writing-new-dockerfiles}

1. Create a new Dockerfile that designates a base image using the [FROM instruction](https://docs.docker.com/engine/reference/builder/#from).

```
FROM apache/beam_python3.7_sdk:2.25.0

ENV FOO=bar
COPY /src/path/to/file /dest/path/to/file/
```

This `Dockerfile` uses the prebuilt Python 3.7 SDK container image [`beam_python3.7_sdk`](https://hub.docker.com/r/apache/beam_python3.7_sdk) tagged at (SDK version) `2.25.0`, and adds an additional environment variable and file to the image.


2. [Build](https://docs.docker.com/engine/reference/commandline/build/) and [push](https://docs.docker.com/engine/reference/commandline/push/) the image using Docker.

  ```
  export BASE_IMAGE="apache/beam_python3.7_sdk:2.25.0"
  export IMAGE_NAME="myremoterepo/mybeamsdk"
  export TAG="latest"

  # Optional - pull the base image into your local Docker daemon to ensure
  # you have the most up-to-date version of the base image locally.
  docker pull "${BASE_IMAGE}"

  docker build -f Dockerfile -t "${IMAGE_NAME}:${TAG}" .
  ```

3. If your runner is running remotely, retag and [push](https://docs.docker.com/engine/reference/commandline/push/) the image to the appropriate repository.

  ```
  docker push "${IMAGE_NAME}:${TAG}"
  ```

4. After pushing a container image, verify the remote image ID and digest matches the local image ID and digest, output from `docker build` or `docker images`.

#### Modifying a source Dockerfile in Beam {#modifying-dockerfiles}

This method requires building image artifacts from Beam source. For additional instructions on setting up your development environment, see the [Contribution guide](/contribute/#development-setup).

>**NOTE**: It is recommended that you start from a stable release branch (`release-X.XX.X`) corresponding to the same version of the SDK to run your pipeline. Differences in SDK version may result in unexpected errors.

1. Clone the `beam` repository.

  ```
  export BEAM_SDK_VERSION="2.26.0"
  git clone https://github.com/apache/beam.git
  cd beam

  # Save current directory as working directory
  export BEAM_WORKDIR=$PWD

  git checkout origin/release-$BEAM_SDK_VERSION
  ```

2. Customize the `Dockerfile` for a given language, typically `sdks/<language>/container/Dockerfile` directory (e.g. the [Dockerfile for Python](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile). If you're adding dependencies from [PyPI](https://pypi.org/), use [`base_image_requirements.txt`](https://github.com/apache/beam/blob/master/sdks/python/container/base_image_requirements.txt) instead.

3. Return to the root Beam directory and run the Gradle `docker` target for your image.

  ```
  cd $BEAM_WORKDIR

  # The default repository of each SDK
  ./gradlew :sdks:java:container:java8:docker
  ./gradlew :sdks:java:container:java11:docker
  ./gradlew :sdks:java:container:java17:docker
  ./gradlew :sdks:go:container:docker
  ./gradlew :sdks:python:container:py36:docker
  ./gradlew :sdks:python:container:py37:docker
  ./gradlew :sdks:python:container:py38:docker
  ./gradlew :sdks:python:container:py39:docker

  # Shortcut for building all Python SDKs
  ./gradlew :sdks:python:container buildAll
  ```

4. Verify the images you built were created by running `docker images`.

  ```
  $> docker images --digests
  REPOSITORY                         TAG                  DIGEST                   IMAGE ID         CREATED           SIZE
  apache/beam_java8_sdk              latest               sha256:...               ...              1 min ago         ...
  apache/beam_java11_sdk             latest               sha256:...               ...              1 min ago         ...
  apache/beam_python3.6_sdk          latest               sha256:...               ...              1 min ago         ...
  apache/beam_python3.7_sdk          latest               sha256:...               ...              1 min ago         ...
  apache/beam_python3.8_sdk          latest               sha256:...               ...              1 min ago         ...
  apache/beam_python3.9_sdk          latest               sha256:...               ...              1 min ago         ...
  apache/beam_go_sdk                 latest               sha256:...               ...              1 min ago         ...
  ```

5. If your runner is running remotely, retag the image and [push](https://docs.docker.com/engine/reference/commandline/push/) the image to your repository. You can skip this step if you provide a custom repo/tag as [additional parameters](#additional-build-parameters).

  ```
  export BEAM_SDK_VERSION="2.26.0"
  export IMAGE_NAME="gcr.io/my-gcp-project/beam_python3.7_sdk"
  export TAG="${BEAM_SDK_VERSION}-custom"

  docker tag apache/beam_python3.7_sdk "${IMAGE_NAME}:${TAG}"
  docker push "${IMAGE_NAME}:${TAG}"
  ```

6. After pushing a container image, verify the remote image ID and digest matches the local image ID and digest output from `docker_images --digests`.

#### Additional build parameters{#additional-build-parameters}

The docker Gradle task defines a default image repository and [tag](https://docs.docker.com/engine/reference/commandline/tag/) is the SDK version defined at [gradle.properties](https://github.com/apache/beam/blob/master/gradle.properties). The default repository is the Docker Hub `apache` namespace, and the default tag is the [SDK version](https://github.com/apache/beam/blob/master/gradle.properties) defined at gradle.properties.

You can specify a different repository or tag for built images by providing parameters to the build task. For example:

```
./gradlew :sdks:python:container:py36:docker -Pdocker-repository-root="example-repo" -Pdocker-tag="2.26.0-custom"
```

builds the Python 3.6 container and tags it as `example-repo/beam_python3.6_sdk:2.26.0-custom`.

From Beam 2.21.0 and later, a `docker-pull-licenses` flag was introduced to add licenses/notices for third party dependencies to the docker images. For example:

```
./gradlew :sdks:java:container:java8:docker -Pdocker-pull-licenses
```
creates a Java 8 SDK image with appropriate licenses in `/opt/apache/beam/third_party_licenses/`.

By default, no licenses/notices are added to the docker images.


## Running pipelines with custom container images {#running-pipelines}

The common method for providing a container image requires using the
PortableRunner flag `--environment_config` as supported by the Portable
Runner or by runners supported PortableRunner flags.
Other runners, such as Dataflow, support specifying containers with different flags.

{{< runner direct >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"
export IMAGE_URL = "${IMAGE}:${TAG}"

python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output /path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URL}"
{{< /runner >}}

{{< runner flink >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"
export IMAGE_URL = "${IMAGE}:${TAG}"

# Run a pipeline using the FlinkRunner which starts a Flink job server.
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=path/to/write/counts \
--runner=FlinkRunner \
# When running batch jobs locally, we need to reuse the container.
--environment_cache_millis=10000 \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URL}"
{{< /runner >}}

{{< runner spark >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"
export IMAGE_URL = "${IMAGE}:${TAG}"

# Run a pipeline using the SparkRunner which starts the Spark job server
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=path/to/write/counts \
--runner=SparkRunner \
# When running batch jobs locally, we need to reuse the container.
--environment_cache_millis=10000 \
--environment_type="DOCKER" \
--environment_config="${IMAGE_URL}"
{{< /runner >}}

{{< runner dataflow >}}
export GCS_PATH="gs://my-gcs-bucket"
export GCP_PROJECT="my-gcp-project"
export REGION="us-central1"

# By default, the Dataflow runner has access to the GCR images
# under the same project.
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"
export IMAGE_URL = "${IMAGE}:${TAG}"

# Run a pipeline on Dataflow.
# This is a Python batch pipeline, so to run on Dataflow Runner V2
# you must specify the experiment "use_runner_v2"

python -m apache_beam.examples.wordcount \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output "${GCS_PATH}/counts" \
  --runner DataflowRunner \
  --project $GCP_PROJECT \
  --region $REGION \
  --temp_location "${GCS_PATH}/tmp/" \
  --experiment=use_runner_v2 \
  --sdk_container_image=$IMAGE_URL

{{< /runner >}}


### Troubleshooting

The following section describes some common issues to consider
when you encounter unexpected errors running Beam pipelines with
custom containers.

* Differences in language and SDK version between the container SDK and
  pipeline SDK may result in unexpected errors due to incompatibility. For best
  results, make sure to use the same stable SDK version for your base container
  and when running your pipeline.
* If you are running into unexpected errors when using remote containers,
  make sure that your container exists in the remote repository and can be
  accessed by any third-party service, if needed.
* Local runners attempt to pull remote images and default to local
  images. If an image cannot be pulled locally (by the docker daemon),
  you may see an log message like:
  ```
  Error response from daemon: manifest for remote.repo/beam_python3.7_sdk:2.25.0-custom not found: manifest unknown: ...
  INFO:apache_beam.runners.portability.fn_api_runner.worker_handlers:Unable to pull image...
  ```
