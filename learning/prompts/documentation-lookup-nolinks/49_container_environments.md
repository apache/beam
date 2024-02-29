Prompt:
How to create customized containers for the Beam SDKs?

Response:
The Apache Beam SDK runtime environment can be containerized with Docker to isolate it from other runtime systems. Public Beam SDK container images are built from Dockerfiles and checked into the Apache Beam GitHub repository for every release.

Customizing publicly available container images may be necessary for various reasons, such as pre-installing additional dependencies or configuring the execution environment further.

To create customized containers for Apache Beam SDKs using one of the three available methods, follow these steps:

***A. Write a New `Dockerfile` Based on a Released Container Image:***

This method is suitable for making simple additions to the image, such as adding artifacts or environment variables.

1. Create a new `Dockerfile` specifying a base image by using the Docker `FROM` instruction:

```Dockerfile
FROM apache/beam_python3.11_sdk:2.54.0
ENV FOO=bar
COPY /src/path/to/file /dest/path/to/file/
```

  This `Dockerfile` uses the prebuilt Python 3.11 SDK container image `beam_python3.11_sdk` tagged at SDK version `2.54.0` and adds an environment variable and file to the image.

  Ensure that the Python or Java runtime version specified in the base image matches the version used in your Apache Beam pipeline.

2. Then build and push the image using Docker:

```bash
export BASE_IMAGE="apache/beam_python3.11_sdk:2.54.0"
export IMAGE_NAME="myremoterepo/mybeamsdk"
export TAG="mybeamsdk-versioned-tag"

docker pull "${BASE_IMAGE}"

docker build -f Dockerfile -t "${IMAGE_NAME}:${TAG}" .
```

3. Make sure to retag and push the image if your runner is running remotely:

```bash
docker push "${IMAGE_NAME}:${TAG}"
```

4. Verify that the image you built was created by running `docker images`.

***B. Modify a Source Dockerfile in Beam:***

This method requires building image artifacts from Beam source but allows for greater customization of the container, including replacement of artifacts or base OS/language versions.

1. Clone the Beam repository and customize the `Dockerfile` for your desired language:

```bash
export BEAM_SDK_VERSION="2.54.0"
git clone https://github.com/apache/beam.git
cd beam

export BEAM_WORKDIR=$PWD

git checkout origin/release-$BEAM_SDK_VERSION
```

  Ensure that the Python or Java runtime version specified in the base image matches the version used in your Apache Beam pipeline.

  Customize the `Dockerfile` for a given language, typically found in the `sdks/<language>/container/Dockerfile` directory (for example, the `Dockerfile` for Python).

2. Then return to the root Beam directory and run the Gradle `docker` target for your image:

```bash
cd $BEAM_WORKDIR

./gradlew :sdks:python:container:py311:docker
```

3. Make sure to retag and push the image if your runner is running remotely:

```bash
docker push "${IMAGE_NAME}:${TAG}"
```

4. Verify the image you built was created by running `docker images`.

***C. Modify an Existing Container Image:***

This method is used when users start from an existing image and configure it to be compatible with Apache Beam runners.

1. Use a multi-stage build process to copy necessary artifacts from a default Apache Beam base image to build your custom container image:

```Dockerfile
FROM python:3.11-bookworm

RUN pip install --no-cache-dir apache-beam[gcp]==2.54.0

COPY --from=apache/beam_python3.11_sdk:2.54.0 /opt/apache/beam /opt/apache/beam

ENTRYPOINT ["/opt/apache/beam/boot"]
```

  Ensure that the Python or Java runtime version specified in the base image matches the version used in your Apache Beam pipeline.

2. Then build and push the image using Docker:

```bash
export BASE_IMAGE="apache/beam_python3.11_sdk:2.54.0"
export IMAGE_NAME="myremoterepo/mybeamsdk"
export TAG="latest"

docker pull "${BASE_IMAGE}"

docker build -f Dockerfile -t "${IMAGE_NAME}:${TAG}" .
```

3. Make sure to retag and push the image if your runner is running remotely:

```bash
docker push "${IMAGE_NAME}:${TAG}"
```

4. Verify that the image you built was created by running `docker images`.

For more details and troubleshooting information, refer to the Container Environments section in the Apache Beam documentation.
