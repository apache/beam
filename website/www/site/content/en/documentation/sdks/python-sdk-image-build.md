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

# Building Beam Python SDK Image Guide

You can build a docker image if your local environment has Java, Python, Golang
and Docker properly. Try
`./gradlew :sdks:python:container:py<PYTHON_VERSION>:docker`. For example,
`:sdks:python:container:py310:docker` builds `apache/beam_python3.10_sdk`
locally if successful. You can follow this guide building a custom image from
a VM if the build fails in your local environment.

## Prepare VM

Prepare a VM with Debian 11. This guide was tested on Debian 11.

### Google Compute Engine

An option to create a Debian 11 VM is using a GCE instance.

```shell
gcloud compute instances create beam-builder \
  --zone=us-central1-a  \
  --image-project=debian-cloud \
  --image-family=debian-11 \
  --machine-type=n1-standard-8 \
  --boot-disk-size=20GB \
  --scopes=cloud-platform
```

Login to the VM. All the following steps are executed inside the VM.

```shell
gcloud compute ssh beam-builder --zone=us-central1-a --tunnel-through-iap
```

Update the apt package list.

```shell
sudo apt-get update
```

> [!NOTE]
> * A high CPU machine is recommended to reduce the compile time.
> * The image build needs a large disk. The build will fail with "no space left
    on device" with the default disk size 10GB.
> * The `cloud-platform` is recommended to avoid permission issues with Google
    Cloud Artifact Registry. You can use the default scopes if you don't push
    the image to Google Cloud Artifact Registry.
> * Use a zone in the region of your docker repository of Artifact Registry if
    you push the image to Artifact Registry.

## Prerequisite Packages

### Java

You need Java to run Gradle tasks.

```shell
sudo apt-get install -y openjdk-11-jdk
```

### Golang

Download and install. Reference: https://go.dev/doc/install.

```shell
# Download and install
curl -OL  https://go.dev/dl/go1.23.2.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.2.linux-amd64.tar.gz

# Add go to PATH.
export PATH=:/usr/local/go/bin:$PATH
```

Confirm the Golang version

```shell
go version
```

Expected output:

```text
go version go1.23.2 linux/amd64
```

> [!NOTE]
> Old Go version (e.g. 1.16) will fail at `:sdks:python:container:goBuild`.

### Python

This guide uses Pyenv to manage multiple Python versions.
Reference: https://realpython.com/intro-to-pyenv/#build-dependencies

```shell
# Install dependencies
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev

# Install Pyenv
curl https://pyenv.run | bash

# Add pyenv to PATH.
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Install Python 3.9 and set the Python version. This will take several minutes.

```shell
pyenv install 3.9
pyenv global 3.9
```

Confirm the python version.

```shell
python --version
```

Expected output example:

```text
Python 3.9.17
```

> [!NOTE]
> You can use a different Python version for building with [
`-PpythonVersion` option](https://github.com/apache/beam/blob/v2.60.0/buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy#L2956-L2961)
> to Gradle task run. Otherwise, you should have `python3.9` in the build
> environment for Apache Beam 2.60.0 or later (python3.8 for older Apache Beam
> versions). If you use the wrong version, the Gradle task
`:sdks:python:setupVirtualenv` fails.

### Docker

Install Docker
following [the reference](https://docs.docker.com/engine/install/debian/#install-using-the-repository).

```shell
# Add GPG keys.
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the Apt repository.
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

# Install docker packages.
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

You need to run `docker` command without the root privilege in Beam Python SDK
image build. You can do this
by [adding your account to the docker group](https://docs.docker.com/engine/install/linux-postinstall/).

```shell
sudo usermod -aG docker $USER
newgrp docker
```

Confirm if you can run a container without the root privilege.

```shell
docker run hello-world
```

### Git

Git is not necessary for building Python SDK image. Git is just used to download
the Apache Beam code in this guide.

```shell
sudo apt-get install -y git
```

## Build Beam Python SDK Image

Download Apache Beam
from [the Github repository](https://github.com/apache/beam).

```shell
git clone https://github.com/apache/beam beam
cd beam
```

Make changes to the Apache Beam code.

Run the Gradle task to start Docker image build. This will take several minutes.
You can run `:sdks:python:container:py<PYTHON_VERSION>:docker` to build an image
for different Python version.
See [the supported version list](https://github.com/apache/beam/tree/master/sdks/python/container).
For example, `py310` is for Python 3.10.

```shell
./gradlew :sdks:python:container:py310:docker
```

If the build is successful, you can see the built image locally.

```shell
docker images
```

Expected output:

```text
REPOSITORY                   TAG       IMAGE ID       CREATED              SIZE
apache/beam_python3.10_sdk   2.60.0    33db45f57f25   About a minute ago   2.79GB
```

> [!NOTE]
> If you run the build in your local environment and Gradle task
`:sdks:python:setupVirtualenv` fails by an incompatible python version, please
> try with `-PpythonVersion` with the Python version installed in your local
> environment (e.g. `-PpythonVersion=3.10`)

## Push to Repository

You may push the custom image to a image repository. The image can be used
for [Dataflow custom container](https://cloud.google.com/dataflow/docs/guides/run-custom-container#usage).

### Google Cloud Artifact Registry

You can push the image to Artifact Registry. No additional authentication is
necessary if you use Google Compute Engine.

```shell
docker tag apache/beam_python3.10_sdk:2.60.0 us-central1-docker.pkg.dev/<MY_PROJECT>/<MY_REPOSITORY>/beam_python3.10_sdk:2.60.0-custom
docker push us-central1-docker.pkg.dev/<MY_PROJECT>/<MY_REPOSITORY>/beam_python3.10_sdk:2.60.0-custom
```

If you push an image in an environment other than a VM in Google Cloud, you
should configure [docker authentication with
`gcloud`](https://cloud.google.com/artifact-registry/docs/docker/authentication#gcloud-helper)
before `docker push`.

### Docker Hub

You can push your Docker hub repository
after [docker login](https://docs.docker.com/reference/cli/docker/login/).

```shell
docker tag apache/beam_python3.10_sdk:2.60.0 <my-account>/beam_python3.10_sdk:2.60.0-custom
docker push <my-account>/beam_python3.10_sdk:2.60.0-custom
```

## (Optional) Update Boot Application Only.

If you only need to make a change to the Python SDK boot application. You can
rebuild the boot application only and include the updated boot application in
the preexisting image.

```shell
# From beam repo root, make changes to boot.go.
your_editor sdks/python/container/boot.go

# Rebuild the entrypoint
./gradlew :sdks:python:container:gobuild

cd sdks/python/container/build/target/launcher/linux_amd64

# Create a simple Dockerfile to use custom boot entrypoint.
cat >Dockerfile <<EOF
FROM apache/beam_python3.10_sdk:2.60.0
COPY boot /opt/apache/beam/boot_modified
ENTRYPOINT ["/opt/apache/beam/boot_modified"]
EOF

# Build the image
docker build . --tag us-central1-docker.pkg.dev/<MY_PROJECT>/<MY_REPOSITORY>/beam_python3.10_sdk:2.60.0-custom-boot
docker push us-central1-docker.pkg.dev/<MY_PROJECT>/<MY_REPOSITORY>/beam_python3.10_sdk:2.60.0-custom-boot
```