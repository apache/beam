###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

# Dockerfile for container with dev prerequisites 


FROM ubuntu

ARG DEBIAN_FRONTEND=noninteractive

# Update and install common packages
RUN apt-get update && apt-get install -y software-properties-common apt-transport-https ca-certificates curl gnupg-agent software-properties-common git vim

# Setup to install docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN apt-key fingerprint 0EBFCD88
RUN  add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

# Repo for different Python versions
RUN  add-apt-repository -y ppa:deadsnakes/ppa


# Install packages
RUN  apt-get install -y openjdk-8-jdk python3-setuptools python3-pip python3.5 python3.6 python3.7 python2.7 virtualenv tox  docker-ce docker-ce-cli containerd.io


# Install Go
RUN mkdir -p /goroot && curl https://dl.google.com/go/go1.15.2.linux-amd64.tar.gz | tar xvzf - -C /goroot --strip-components=1

# Set environment variables for Go
ENV GOROOT /goroot
ENV GOPATH /gopath
ENV PATH $GOROOT/bin:$GOPATH/bin:$PATH
CMD go get github.com/linkedin/goavro

# Install grpcio-tools mypy-protobuf for `python3 sdks/python/setup.py sdist` to work
RUN pip3 install grpcio-tools mypy-protobuf

# Set work directory
WORKDIR /workspaces/beam
