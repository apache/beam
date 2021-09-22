################################################################################
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
################################################################################

FROM ubuntu:20.04

ARG RUNNER_VERSION=2.281.1

RUN apt-get -yqq update && apt-get install -yqq curl sudo git jq tar gnupg2 apt-transport-https ca-certificates
RUN apt-get install -yqq liblttng-ust0 libkrb5-3 zlib1g libssl1.1
RUN DEBIAN_FRONTEND=noninteractive apt-get install -yqq libicu66
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
RUN useradd -m actions && usermod -aG sudo actions && echo "%sudo ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER actions
WORKDIR /home/actions

RUN curl -O -L https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-x64-$RUNNER_VERSION.tar.gz
RUN tar xzf ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz

COPY --chown=actions:actions entrypoint.sh ./entrypoint.sh

RUN sudo chmod u+x ./entrypoint.sh

ENTRYPOINT ["/home/actions/entrypoint.sh"]


