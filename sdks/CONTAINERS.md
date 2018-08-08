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

Run Gradle with the `docker` target:

```
$ pwd
[...]/beam
$ ./gradlew docker
[...]
> Task :sdks:python:container:docker 
a571bb44bc32: Verifying Checksum
a571bb44bc32: Download complete
aa6d783919f6: Verifying Checksum
aa6d783919f6: Download complete
f2b6b4884fc8: Verifying Checksum
f2b6b4884fc8: Download complete
f2b6b4884fc8: Pull complete
74eaa8be7221: Pull complete
2d6e98fe4040: Pull complete
414666f7554d: Pull complete
bb0bcc8d7f6a: Pull complete
a571bb44bc32: Pull complete
aa6d783919f6: Pull complete
Digest: sha256:d9455be2cc68ded908084ec5b63a5cbb87f12ec0915c2f146751bd50b9aef01a
Status: Downloaded newer image for python:2
 ---> 2863c80c418c
Step 2/6 : MAINTAINER "Apache Beam <dev@beam.apache.org>"
 ---> Running in c787617f4af1
Removing intermediate container c787617f4af1
 ---> b4ffbbf94717
[...]
 ---> a77003ead1a1
Step 5/6 : ADD target/linux_amd64/boot /opt/apache/beam/
 ---> 4998013b3d63
Step 6/6 : ENTRYPOINT ["/opt/apache/beam/boot"]
 ---> Running in 30079dc4204b
Removing intermediate container 30079dc4204b
 ---> 4ea515403a1a
Successfully built 4ea515403a1a
Successfully tagged herohde-docker-apache.bintray.io/beam/python:latest
[...]
```

Note that the container images include built content, including the Go boot
code. Some images, notably python, take a while to build, so building just
the specific images needed can be a lot faster:

```
$ ./gradlew -p sdks/java/container docker
$ ./gradlew -p sdks/python/container docker
$ ./gradlew -p sdks/go/container docker
```

**(Optional)** When built, you can see, inspect and run them locally:

```
$ docker images
REPOSITORY                                       TAG                    IMAGE ID            CREATED       SIZE
herohde-docker-apache.bintray.io/beam/python     latest             4ea515403a1a      3 minutes ago     1.27GB
herohde-docker-apache.bintray.io/beam/java       latest             0103512f1d8f     34 minutes ago      780MB
herohde-docker-apache.bintray.io/beam/go         latest             ce055985808a     35 minutes ago      121MB
[...]
```

Despite the names, these container images live only on your local machine.
While we will re-use the same tag "latest" for each build, the
images IDs will change.

**(Optional)**: the default setting for `docker-repository-root` specifies the above bintray
location. You can override it by adding: 

```
-Pdocker-repository-root=<location>
```

Similarly, if you want to specify a specific tag instead of "latest", such as a "2.3.0"
version, you can do so by adding:

```
-Pdocker-tag=<tag>
```

### Adding dependencies, and making Python go vroom vroom

Not all dependencies are like insurance on used Vespa, if you don't have them some job's just won't run at all and you can't sweet talk your way out of a tensorflow dependency. On the other hand, for Python users dependencies can be automatically installed at run time on each container, which is a great way to find out what your systems timeout limits are. Regardless as to if you have dependency which isn't being installed for you and you need, or you just don't want to install tensorflow 1.6.0 every time you start a new worker this can help.

For Python we have a sample Dockerfile which will take the user specified requirements and install them on top of your base image. If your building from source follow the directions above, otherwise you can set the environment variable BASE_PYTHON_CONTAINER_IMAGE to the desired released version.

```
USER_REQUIREMENTS=~/my_req.txt ./sdks/python/scripts/add_requirements.sh
```

Once your custom container is built, remember to upload it to the registry of your choice.

If you build a custom container when you run your job you will need to specify instead of the default latest container, so for example Holden would specify:

```
--worker_harness_container_image=holden-docker-apache.bintray.io/beam/python-with-requirements
```

## How to push container images

**Preprequisites**: obtain a docker registry account and ensure docker can push images to it,
usually by doing `docker login` with the appropriate information. The image you want
to push must also be present in the local docker image repository.

For the Python SDK harness container image, run:

```
$ docker push $USER-docker-apache.bintray.io/beam/python:latest
The push refers to repository [herohde-docker-apache.bintray.io/beam/python]
723b66d57e21: Pushed 
12d5806e6806: Pushed 
b394bd077c6e: Pushed 
ca82a2274c57: Pushed 
de2fbb43bd2a: Pushed 
4e32c2de91a6: Pushed 
6e1b48dc2ccc: Pushed 
ff57bdb79ac8: Pushed 
6e5e20cbf4a7: Pushed 
86985c679800: Pushed 
8fad67424c4e: Pushed 
latest: digest: sha256:86ad57055324457c3ea950f914721c596c7fa261c216efb881d0ca0bb8457535 size: 2646
```

Similarly for the Java and Go SDK harness container images. If you want to push the same image
to multiple registries, you can retag the image using `docker tag` and push.

**(Optional)** On any machine, you can now pull the pushed container image:

```
$ docker pull $USER-docker-apache.bintray.io/beam/python:latest
latest: Pulling from beam/python
f2b6b4884fc8: Pull complete 
4fb899b4df21: Pull complete 
74eaa8be7221: Pull complete 
2d6e98fe4040: Pull complete 
414666f7554d: Pull complete 
bb0bcc8d7f6a: Pull complete 
a571bb44bc32: Pull complete 
aa6d783919f6: Pull complete 
7255d71dee8f: Pull complete 
08274803455d: Pull complete 
ef79fab5686a: Pull complete 
Digest: sha256:86ad57055324457c3ea950f914721c596c7fa261c216efb881d0ca0bb8457535
Status: Downloaded newer image for herohde-docker-apache.bintray.io/beam/python:latest
$ docker images
REPOSITORY                                     TAG                 IMAGE ID            CREATED          SIZE
herohde-docker-apache.bintray.io/beam/python   latest          4ea515403a1a     35 minutes ago       1.27 GB
[...]
```

Note that the image IDs and digests match their local counterparts.
