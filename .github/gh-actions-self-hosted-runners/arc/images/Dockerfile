#
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
#


FROM ghcr.io/actions-runner-controller/actions-runner-controller/actions-runner:ubuntu-20.04

USER root
#Install Node
RUN curl -OL https://nodejs.org/dist/v18.16.0/node-v18.16.0-linux-x64.tar.xz && \
    tar -C /usr/local -xf node-v18.16.0-linux-x64.tar.xz && \
    rm node-v18.16.0-linux-x64.tar.xz && \
    mv /usr/local/node-v18.16.0-linux-x64 /usr/local/node
ENV PATH="${PATH}:/usr/local/node/bin"
#Install Go
ARG go_version=1.20.5
RUN curl -OL https://go.dev/dl/go${go_version}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${go_version}.linux-amd64.tar.gz && \
    rm go${go_version}.linux-amd64.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin"
#Install Java
RUN curl -OL https://cdn.azul.com/zulu/bin/zulu8.70.0.23-ca-jdk8.0.372-linux_x64.tar.gz && \
    tar -C /usr/local -xzf zulu8.70.0.23-ca-jdk8.0.372-linux_x64.tar.gz && \
    rm zulu8.70.0.23-ca-jdk8.0.372-linux_x64.tar.gz && \
    mv /usr/local/zulu8.70.0.23-ca-jdk8.0.372-linux_x64 /usr/local/java
ENV PATH="${PATH}:/usr/local/java/bin"
#Install Gradle
RUN curl -OL https://services.gradle.org/distributions/gradle-7.3.3-bin.zip && \
    unzip gradle-7.3.3-bin.zip && \
    rm gradle-7.3.3-bin.zip && \
    mv gradle-7.3.3 /usr/local/gradle
ENV PATH="${PATH}:/usr/local/gradle/bin"


# Needed to transfer path addtitions to runner environment
RUN echo PATH=$PATH >> /runnertmp/.env
USER runner
