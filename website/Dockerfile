# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This image contains Hugo and dependencies required to build and test the Beam
# website. It is used by tasks in build.gradle.

FROM debian:stable-slim

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ENV DEBIAN_FRONTEND=noninteractive \
    LANGUAGE=C.UTF-8 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 \
    LC_MESSAGES=C.UTF-8

WORKDIR /opt/

# Install deps being used by sh files
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        gnupg2 \
        gosu \
        lynx \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install node LTS environment
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        nodejs \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

RUN npm update -g npm
    
RUN npm install postcss postcss-cli autoprefixer 

# Install yarn
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends yarn \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install hugo extended version v0.117.0
RUN HUGOHOME="$(mktemp -d)" \
    && export HUGOHOME \
    && curl -sL https://github.com/gohugoio/hugo/releases/download/v0.117.0/hugo_extended_0.117.0_Linux-64bit.tar.gz > "${HUGOHOME}/hugo.tar.gz" \
    && tar -xzvf "${HUGOHOME}/hugo.tar.gz" hugo \
    && mv hugo /usr/local/bin/hugo \
    && chmod +x /usr/local/bin/hugo \
    && rm -r "${HUGOHOME}"

# Prevent git errors of the form "fatal: detected dubious ownership in repository"
RUN git config --file=/.gitconfig --add safe.directory /opt && chmod a+r /.gitconfig
