
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dockerfile for installing the necessary dependencies for building Hadoop.
# See BUILDING.txt.

FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true


# hadolint ignore=DL3008
###
# Update and install common packages
###
RUN apt -q update \
   && apt install -y software-properties-common apt-utils apt-transport-https ca-certificates \
   && add-apt-repository -y ppa:deadsnakes/ppa \
   && apt -q update

RUN mkdir /package
COPY pkglist /package/pkglist
RUN apt-get -q install -y --no-install-recommends $(grep -v '^#' /package/pkglist | cat)

###
# Set the locale ( see https://stackoverflow.com/a/28406007/114196 )
###
# TODO(BEAM-11327): Remove the need to run tests with UTF-8 encoding
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

###
# Set Python3.6 as default
###
RUN alias python=python3.6

###
# Install grpcio-tools mypy-protobuf for `python3 sdks/python/setup.py sdist` to work
###
RUN pip3 install grpcio-tools mypy-protobuf

###
# Install useful tools
# Install distlib to avoid https://github.com/pypa/virtualenv/issues/2006
RUN pip3 install distlib==0.3.1 yapf==0.29.0 pytest
###

###
# Install Go
###
ENV DOWNLOAD_GO_VERSION=1.22.5
RUN wget https://golang.org/dl/go${DOWNLOAD_GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${DOWNLOAD_GO_VERSION}.linux-amd64.tar.gz
ENV GOROOT /usr/local/go
ENV PATH $PATH:$GOROOT/bin

###
# Add a welcome message and environment checks.
###
RUN mkdir /scripts
COPY beam_env_checks.sh /scripts/beam_env_checks.sh
COPY bashcolors.sh      /scripts/bashcolors.sh
RUN chmod 755 /scripts /scripts/beam_env_checks.sh /scripts/bashcolors.sh

# hadolint ignore=SC2016
RUN echo '. /etc/bash_completion'        >> /root/.bash_aliases
RUN echo '. /scripts/beam_env_checks.sh' >> /root/.bash_aliases
