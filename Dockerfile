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


FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

# Update and install common packages
RUN apt -q update \
   && apt install -y software-properties-common apt-utils apt-transport-https ca-certificates \
   && add-apt-repository -y ppa:deadsnakes/ppa \
   && apt-get -q install -y --no-install-recommends \
      curl \
      gnupg-agent \
      rsync \
      git \
      vim \
      locales \
      wget \
      time \
      openjdk-8-jdk \
      python3-setuptools \
      python3-pip \
      python3.5 \
      python3.6 \
      python3.7 \
      python2.7 \
      virtualenv \
      tox  

# Setup to install docker
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN apt-key fingerprint 0EBFCD88
RUN  add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
RUN  apt-get install -y docker-ce docker-ce-cli containerd.io

# Set the locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

#Set Python3.6 as default
RUN alias python=python3.6

# Install Go
RUN wget https://golang.org/dl/go1.15.5.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.15.5.linux-amd64.tar.gz
# ENV PATH $PATH:/usr/local/go/bin
ENV GOROOT /usr/local/go
ENV PATH $PATH:$GOROOT/bin

# Set work directory
WORKDIR /workspaces/beam
ENV GOPATH /workspaces/beam/sdks/go/examples/.gogradle/project_gopath
RUN go get github.com/linkedin/goavro

# Install grpcio-tools mypy-protobuf for `python3 sdks/python/setup.py sdist` to work
RUN pip3 install grpcio-tools mypy-protobuf
