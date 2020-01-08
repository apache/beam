---
layout: section
title: "Container environments"
section_menu: section-menu/documentation.html
permalink: /documentation/runtime/environments/
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

The Beam SDK runtime environment is isolated from other runtime systems because the SDK runtime environment is [containerized](https://s.apache.org/beam-fn-api-container-contract) with [Docker](https://www.docker.com/). This means that any execution engine can run the Beam SDK.

This page describes how to customize, build, and push Beam SDK container images.

Before you begin, install [Docker](https://www.docker.com/) on your workstation.

## Customizing container images

You can add extra dependencies to container images so that you don't have to supply the dependencies to execution engines.

To customize a container image, either:
* [Write a new](#writing-new-dockerfiles) [Dockerfile](https://docs.docker.com/engine/reference/builder/) on top of the original.
* [Modify](#modifying-dockerfiles) the [original Dockerfile](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile) and reimage the container.

It's often easier to write a new Dockerfile. However, by modifying the original Dockerfile, you can customize anything (including the base OS).

### Writing new Dockerfiles on top of the original {#writing-new-dockerfiles}

1. Pull a [prebuilt SDK container image](https://hub.docker.com/u/apachebeam) for your [target](https://docs.docker.com/docker-hub/repos/#searching-for-repositories) language and version. The following example pulls the latest Python SDK:
```
docker pull apachebeam/python3.7_sdk
```
2. [Write a new Dockerfile](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/) that [designates](https://docs.docker.com/engine/reference/builder/#from) the original as its [parent](https://docs.docker.com/glossary/?term=parent%20image).
3. [Build](#building-container-images) a child image.

### Modifying the original Dockerfile {#modifying-dockerfiles}

1. Clone the `beam` repository:
```
git clone https://github.com/apache/beam.git
```
2. Customize the [Dockerfile](https://github.com/apache/beam/blob/master/sdks/python/container/Dockerfile). If you're adding dependencies from [PyPI](https://pypi.org/), use [`base_image_requirements.txt`](https://github.com/apache/beam/blob/master/sdks/python/container/base_image_requirements.txt) instead.
3. [Reimage](#building-container-images) the container.

### Testing customized images

To test a customized image locally, run a pipeline with PortableRunner and set the `--environment_config` flag to the image path:

{:.runner-direct}

```
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output /path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=embed \
--environment_config=path/to/container/image
```

{:.runner-flink-local}

```
# Start a Flink job server on localhost:8099
./gradlew :runners:flink:1.7:job-server:runShadow

# Run a pipeline on the Flink job server
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=/path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
--environment_config=path/to/container/image
```

{:.runner-spark-local}

```
# Start a Spark job server on localhost:8099
./gradlew :runners:spark:job-server:runShadow

# Run a pipeline on the Spark job server
python -m apache_beam.examples.wordcount \
--input=/path/to/inputfile \
--output=path/to/write/counts \
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
--environment_config=path/to/container/image
```

To test a customized image on the Google Cloud Dataflow runner, use
`DataflowRunner` with the `beam_fn_api` experiment and set
`worker_harness_container_image` to the custom container:

```
python -m apache_beam.examples.wordcount \ 
--input=path/to/inputfile \
--output=/path/to/write/counts \
--runner=DataflowRunner \
--project={gcp_project_id} \
--temp_location={gcs_location} \ \
--experiment=beam_fn_api \
--sdk_location=[…]/beam/sdks/python/container/py{version}/build/target/apache-beam.tar.gz \
--worker_harness_container_image=path/to/container/image

# The sdk_location option accepts four Python version variables: 2, 35, 36, and 37
```

## Building container images

To build Beam SDK container images:

1. Navigate to the local copy of your [customized container image](#customizing-container-images).
2. Run Gradle with the `docker` target. If you're [building a child image](#writing-new-dockerfiles), set the optional `--file` flag to the new Dockerfile. If you're [building an image from an original Dockerfile](#modifying-dockerfiles), ignore the `--file` flag and use a default repository:

```
# The default repository of each SDK
./gradlew [--file=path/to/new/Dockerfile] :sdks:java:container:docker
./gradlew [--file=path/to/new/Dockerfile] :sdks:go:container:docker
./gradlew [--file=path/to/new/Dockerfile] :sdks:python:container:py2:docker
./gradlew [--file=path/to/new/Dockerfile] :sdks:python:container:py35:docker
./gradlew [--file=path/to/new/Dockerfile] :sdks:python:container:py36:docker
./gradlew [--file=path/to/new/Dockerfile] :sdks:python:container:py37:docker

# Shortcut for building all four Python SDKs
./gradlew [--file=path/to/new/Dockerfile] :sdks:python:container buildAll
```

To examine the containers that you built, run `docker images` from anywhere in the command line. If you successfully built all of the container images, the command prints a table like the following:
```
REPOSITORY                          TAG                 IMAGE ID            CREATED           SIZE
apachebeam/java_sdk                 latest              16ca619d489e        2 weeks ago        550MB
apachebeam/python2.7_sdk            latest              b6fb40539c29        2 weeks ago       1.78GB
apachebeam/python3.5_sdk            latest              bae309000d09        2 weeks ago       1.85GB
apachebeam/python3.6_sdk            latest              42faad307d1a        2 weeks ago       1.86GB
apachebeam/python3.7_sdk            latest              18267df54139        2 weeks ago       1.86GB
apachebeam/go_sdk                   latest              30cf602e9763        2 weeks ago        124MB
```

### Overriding default Docker targets

The default [tag](https://docs.docker.com/engine/reference/commandline/tag/) is `latest` and the default repositories are in the Docker Hub `apachebeam` namespace. The `docker` command-line tool implicitly [pushes container images](#pushing-container-images) to this location.

To tag a local image, set the `docker-tag` option when building the container. The following command tags a Python SDK image with a date.
```
./gradlew :sdks:python:container:py2:docker -Pdocker-tag=2019-10-04
```

To change the repository, set the `docker-repository-root` option to a new location. The following command sets the `docker-repository-root` to a Bintray repository named `apache`.
```
./gradlew :sdks:python:container:py2:docker -Pdocker-repository-root=$USER-docker-apache.bintray.io/beam/python
```

## Pushing container images

After [building a container image](#building-container-images), you can store it in a remote Docker repository.

The following steps push a Python SDK image to the [`docker-root-repository` value](#overriding-default-docker-targets).

1. Sign in to your Docker registry:
```
docker login
```
2. Navigate to the local copy of your container image and upload it to the remote repository:
```
docker push apachebeam/python2.7_sdk
```

To download the image again, run `docker pull`:
```
docker pull apachebeam/python2.7_sdk
```

> **Note**: After pushing a container image, the remote image ID and digest match the local image ID and digest.