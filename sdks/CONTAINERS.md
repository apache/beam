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

# Docker containers

The Beam [portability effort](https://s.apache.org/beam-fn-api) aims to make it possible
for any SDK to work with any runner. One aspect of the effort is the isolation of the SDK
and user code execution environment from the runner execution environment using
[docker](https://www.docker.com/), as defined in the portability
[container contract](https://s.apache.org/beam-fn-api-container-contract).

This document describes how to build and push container images to that end. The push
step generally requires an account with a public docker registry, such
as [bintray.io](bintray.io) or
[Google Container Registry](https://cloud.google.com/container-registry). These
instructions assume familiarity with docker and a bintray account under the
current username with a docker repository named "apache".

## How to build container images

**Prerequisites**: install [docker](https://www.docker.com/) on your
platform. You can verify that it works by running `docker images` or any other
docker command.

Run Maven with the `build-containers` profile:

```
$ pwd
[...]/beam
$ mvn clean install -DskipTests -Pbuild-containers
[...]
[INFO] --- dockerfile-maven-plugin:1.3.5:build (default) @ beam-sdks-python-container ---
[INFO] Using Google application default credentials
[INFO] loaded credentials for user account with clientId=[...].apps.googleusercontent.com
[INFO] Building Docker context /Users/herohde/go/src/github.com/apache/beam/sdks/python/container
[INFO] 
[INFO] Image will be built as herohde-docker-apache.bintray.io/beam/python:latest
[INFO] 
[INFO] Step 1/4 : FROM python:2
[INFO] Pulling from library/python
[INFO] Digest: sha256:181ee8edfd9d44323c82dcba0b187f1ee2eb3d4a11c8398fc06952ed5f9ef32c
[INFO] Status: Image is up to date for python:2
[INFO]  ---> b1d5c2d7dda8
[INFO] Step 2/4 : MAINTAINER "Apache Beam <dev@beam.apache.org>"
[INFO]  ---> Running in f1bc3c4943b3
[INFO]  ---> 9867b512e47e
[INFO] Removing intermediate container f1bc3c4943b3
[INFO] Step 3/4 : ADD target/linux_amd64/boot /opt/apache/beam/
[INFO]  ---> 5cb81c3d2d90
[INFO] Removing intermediate container 4a41ad80005a
[INFO] Step 4/4 : ENTRYPOINT /opt/apache/beam/boot
[INFO]  ---> Running in 40f5b945afe7
[INFO]  ---> c8bf712741c8
[INFO] Removing intermediate container 40f5b945afe7
[INFO] Successfully built c8bf712741c8
[INFO] Successfully tagged herohde-docker-apache.bintray.io/beam/python:latest
[INFO] 
[INFO] Detected build of image with id c8bf712741c8
[INFO] Building jar: /Users/herohde/go/src/github.com/apache/beam/sdks/python/container/target/beam-sdks-python-container-2.3.0-SNAPSHOT-docker-info.jar
[INFO] Successfully built herohde-docker-apache.bintray.io/beam/python:latest
[INFO]
[...]
```

Note that the container images include built content, including the Go boot
code, so you should build from the top level directory unless you're familiar
with Maven.

**(Optional)** When built, you can see, inspect and run them locally:

```
$ docker images
REPOSITORY                                       TAG                    IMAGE ID            CREATED             SIZE
herohde-docker-apache.bintray.io/beam/python     latest                 c8bf712741c8        About an hour ago   690MB
herohde-docker-apache.bintray.io/beam/java       latest                 33efc0947952        About an hour ago   773MB
[...]
```

Despite the names, these container images live only on your local machine.
While we will re-use the same tag "latest" for each build, the
images IDs will change.

**(Optional)**: the default setting for `docker-repository-root` specifies the above bintray
location. You can override it by adding: 

```
-Ddocker-repository-root=<location>
```

Similarly, if you want to specify a specific tag instead of "latest", such as a "2.3.0"
version, you can do so by adding:

```
-Ddockerfile.tag=<tag>
```

## How to push container images

**Preprequisites**: obtain a docker registry account and ensure docker can push images to it,
usually by doing `docker login` with the appropriate information. The image you want
to push must also be present in the local docker image repository.

For the Python SDK harness container image, run:

```
$ docker push $USER-docker-apache.bintray.io/beam/python:latest
The push refers to a repository [herohde-docker-apache.bintray.io/beam/python]
f2a8798331f5: Pushed 
6b200cb2b684: Layer already exists 
bf56c6510f38: Layer already exists 
7890d67efa6f: Layer already exists 
b456afdc9996: Layer already exists 
d752a0310ee4: Layer already exists 
db64edce4b5b: Layer already exists 
d5d60fc34309: Layer already exists 
c01c63c6823d: Layer already exists 
latest: digest: sha256:58da4d9173a29622f0572cfa22dfeafc45e6750dde4beab57a47a9d1d17d601b size: 2222

```

Similarly for the Java SDK harness container image. If you want to push the same image
to multiple registries, you can retagging the image using `docker tag` and push.

**(Optional)** On any machine, you can now pull the pushed container image:

```
$ docker pull $USER-docker-apache.bintray.io/beam/python:latest
latest: Pulling from beam/python
85b1f47fba49: Pull complete 
5409e9a7fa9e: Pull complete 
661393707836: Pull complete 
1bb98c08d57e: Pull complete 
c842a08369e2: Pull complete 
310408aa843f: Pull complete 
d6a27cfc2cf1: Pull complete 
7a24cf0c9043: Pull complete 
290b127dfe35: Pull complete 
Digest: sha256:58da4d9173a29622f0572cfa22dfeafc45e6750dde4beab57a47a9d1d17d601b
Status: Downloaded newer image for herohde-docker-apache.bintray.io/beam/python:latest
$ docker images
REPOSITORY                                     TAG                 IMAGE ID            CREATED             SIZE
herohde-docker-apache.bintray.io/beam/python   latest              c8bf712741c8        2 hours ago         690MB
[...]
```

Note that the image IDs and digests match their local counterparts.
