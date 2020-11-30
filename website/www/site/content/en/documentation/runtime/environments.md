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

The Beam SDK runtime environment is [containerized](https://www.docker.com/resources/what-container) with [Docker](https://www.docker.com/) to isolate it from other runtime systems. This means any execution engine can run the Beam SDK. To learn more about the container environment, read the Beam [SDK Harness container contract](https://s.apache.org/beam-fn-api-container-contract).

Prebuilt SDK container images are released per supported language during Beam releases and pushed to [Docker Hub](https://hub.docker.com/search?q=apache%2Fbeam&type=image)

## Custom containers

You may want to customize container images for many reasons, including:

* Pre-installing additional dependencies,
* Launching third-party software
* Further customizing the execution environment

 This guide describes how to create and use customized containers for the Beam SDK.

### Prerequisites

* You will need to have [Docker installed](https://docs.docker.com/get-docker/).
* You will need to have a container registry accessible by your execution engine or runner to host a custom container image. Options include [Docker Hub](https://hub.docker.com/) or a "self-hosted" repository, including cloud-specific container registries like [Google Container Registry](https://cloud.google.com/container-registry) (GCR) or [Amazon Elastic Container Registry](https://aws.amazon.com/ecr/) (ECR).

>  **NOTE**: On Nov 20, 2020, Docker Hub put [rate limits](https://www.docker.com/increase-rate-limits) into effect for anonymous and free authenticated use, which may impact larger pipelines that pull containers several times.

### Building and pushing custom containers

Beam [SDK container images](https://hub.docker.com/search?q=apache%2Fbeam&type=image) are built from Dockerfiles checked into the [Github](https://github.com/apache/beam) repository and published to Docker Hub for every release. You can build customized containers in one of two ways:

1. **[Writing a new](#writing-new-dockerfiles) Dockerfile based on an existing prebuilt container image**. This is sufficient for simple additions to the image, such as adding artifacts or environment variables.
2. **[Modifying](#modifying-dockerfiles) a source Dockerfile in [Beam](https://github.com/apache/beam)**. This method requires building from Beam source but allows for greater customization of the container (including replacement of artifacts or base OS/language versions).

#### Writing a new Dockerfile based on an existing published container image {#writing-new-dockerfiles}

Steps:

1. Create a new Dockerfile that designates a base image using the [FROM instruction](https://docs.docker.com/engine/reference/builder/#from). As an example, this `Dockerfile`:

```
FROM apache/beam_python3.7_sdk:2.25.0

ENV FOO=bar
COPY /src/path/to/file /dest/path/to/file/
```

uses the prebuilt Python 3.7 SDK container image [`beam_python3.7_sdk`](https://hub.docker.com/r/apache/beam_python3.7_sdk) tagged at (SDK version) `2.25.0`, and adds an additional environment variable and file to the image.


2. [Build](https://docs.docker.com/engine/reference/commandline/build/) and [push](https://docs.docker.com/engine/reference/commandline/push/) the image using Docker.


```
export BASE_IMAGE="apache/beam_python3.7_sdk:2.25.0"
export IMAGE_NAME="myremoterepo/mybeamsdk"
export TAG="latest"

# Optional - pull the base image into your local Docker daemon to ensure
# you have the most up-to-date version of the base image locally.
docker pull "${BASE_IMAGE}"

docker build -f Dockerfile -t "${IMAGE_NAME}:${TAG}" .
docker push "${IMAGE_NAME}:${TAG}"
```

**NOTE**: After pushing a container image, you should verify the remote image ID and digest should match the local image ID and digest, output from `docker build` or `docker images`.

#### Modifying a source Dockerfile {#modifying-dockerfiles} in Beam

This method will require building image artifacts from Beam source. For additional instructions on setting up your development environment, see the [Contribution guide](contribute/#development-setup).

1. Clone the `beam` repository.

```
git clone https://github.com/apache/beam.git
```

2. Customize the `Dockerfile` for a given language. This file is typically in the `sdks/<language>/container` directory (e.g. the [Dockerfile for Python](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile). If you're adding dependencies from [PyPI](https://pypi.org/), use [`base_image_requirements.txt`](https://github.com/apache/beam/blob/master/sdks/python/container/base_image_requirements.txt) instead.

3. Navigate to the root directory of the local copy of your Apache Beam.

4. Run Gradle with the `docker` target.


```
# The default repository of each SDK
./gradlew :sdks:java:container:java8:docker
./gradlew :sdks:java:container:java11:docker
./gradlew :sdks:go:container:docker
./gradlew :sdks:python:container:py36:docker
./gradlew :sdks:python:container:py37:docker
./gradlew :sdks:python:container:py38:docker

# Shortcut for building all Python SDKs
./gradlew :sdks:python:container buildAll
```

To examine the containers that you built, run `docker images`:

```
$> docker images
REPOSITORY                         TAG                 IMAGE ID            CREATED           SIZE
apache/beam_java8_sdk              latest              ...                 1 min ago         ...
apache/beam_java11_sdk             latest              ...                 1 min ago         ...
apache/beam_python3.6_sdk          latest              ...                 1 min ago         ...
apache/beam_python3.7_sdk          latest              ...                 1 min ago         ...
apache/beam_python3.8_sdk          latest              ...                 1 min ago         ...
apache/beam_go_sdk                 latest              ...                 1 min ago         ...
```

If you did not provide a custom repo/tag as additional parameters (see below), you can retag the image and [push](https://docs.docker.com/engine/reference/commandline/push/) the image using Docker to a remote repository.

```
export IMAGE_NAME="myrepo/mybeamsdk"
export TAG="latest"

docker tag apache/beam_python3.6_sdk "${IMAGE_NAME}:${TAG}"
docker push "${IMAGE_NAME}:${TAG}"
```

**NOTE**: After pushing a container image, verify the remote image ID and digest matches the local image ID and digest output from `docker_images`

##### Additional build parameters

The docker Gradle task defines a default image repository and [tag](https://docs.docker.com/engine/reference/commandline/tag/) is the SDK version defined at [gradle.properties](https://github.com/apache/beam/blob/master/gradle.properties). The default repository is the Docker Hub `apache` namespace, and the default tag is the [SDK version](https://github.com/apache/beam/blob/master/gradle.properties) defined at gradle.properties. With these settings, the
`docker` command-line tool will implicitly try to push the container to the Docker Hub Apache repository.

You can specify a different repository or tag for built images by providing parameters to the build task. For example:

```
./gradlew :sdks:python:container:py36:docker -Pdocker-repository-root=example-repo -Pdocker-tag=2019-10-04
```

builds the Python 3.6 container and tags it as `example-repo/beam_python3.6_sdk:2019-10-04`.

From Beam 2.21.0 and later, a `docker-pull-licenses` flag was introduced to add licenses/notices for third party dependencies to the docker images. For example:

```
./gradlew :sdks:java:container:java8:docker -Pdocker-pull-licenses
```
creates a Java 8 SDK image with appropriate licenses in `/opt/apache/beam/third_party_licenses/`.

By default, no licenses/notices are added to the docker images.


## Using container images in pipelines

The common method for providing a container image requires using the PortableRunner and setting the `--environment_config` flag to a given image path.
Other runners, such as Dataflow, support specifying containers with different flags.

>  **NOTE**: The Dataflow runner requires Beam SDK version >= 2.21.0.

{{< highlight class="runner-direct" >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"

python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output /path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_config="${IMAGE}:${TAG}"
{{< /highlight >}}

{{< highlight class="runner-flink-local" >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"

# Start a Flink job server on localhost:8099
./gradlew :runners:flink:1.8:job-server:runShadow

# Run a pipeline on the Flink job server
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=/path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
--environment_config="${IMAGE}:${TAG}"
{{< /highlight >}}

{{< highlight class="runner-spark-local" >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"

# Start a Spark job server on localhost:8099
./gradlew :runners:spark:job-server:runShadow

# Run a pipeline on the Spark job server
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
--environment_config="${IMAGE}:${TAG}"
{{< /highlight >}}

{{< highlight class="runner-dataflow" >}}
export IMAGE="my-repo/beam_python_sdk_custom"
export TAG="X.Y.Z"

export GCS_PATH="gs://my-gcs-bucket"
export GCP_PROJECT="my-gcp-project"
export REGION="us-central1"

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
  --worker_harness_container_image="${IMAGE}:${TAG}"

{{< /highlight >}}
