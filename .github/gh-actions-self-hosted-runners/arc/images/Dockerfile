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
#install BuildX
COPY --from=docker/buildx-bin:0.11.1 /buildx /usr/libexec/docker/cli-plugins/docker-buildx
RUN docker buildx install && docker buildx version


USER root
#Install Node
RUN curl -OL https://nodejs.org/dist/v18.16.0/node-v18.16.0-linux-x64.tar.xz && \
    tar -C /usr/local -xf node-v18.16.0-linux-x64.tar.xz && \
    rm node-v18.16.0-linux-x64.tar.xz && \
    mv /usr/local/node-v18.16.0-linux-x64 /usr/local/node
ENV PATH="${PATH}:/usr/local/node/bin"
#Install Go
ARG go_version=1.22.5
RUN curl -OL https://go.dev/dl/go${go_version}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${go_version}.linux-amd64.tar.gz && \
    rm go${go_version}.linux-amd64.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin"
#Install Java
RUN curl -OL https://cdn.azul.com/zulu/bin/zulu11.74.15-ca-jdk11.0.24-linux_x64.tar.gz && \
    tar -C /usr/local -xzf zulu11.74.15-ca-jdk11.0.24-linux_x64.tar.gz && \
    rm zulu11.74.15-ca-jdk11.0.24-linux_x64.tar.gz && \
    mv /usr/local/zulu11.74.15-ca-jdk11.0.24-linux_x64 /usr/local/java
ENV PATH="${PATH}:/usr/local/java/bin"
#Install Gradle
RUN curl -OL https://services.gradle.org/distributions/gradle-8.4-bin.zip && \
    unzip gradle-8.4-bin.zip && \
    rm gradle-8.4-bin.zip && \
    mv gradle-8.4 /usr/local/gradle
ENV PATH="${PATH}:/usr/local/gradle/bin"
#Install GitHub CLI
RUN curl -OL https://github.com/cli/cli/releases/download/v2.31.0/gh_2.31.0_linux_amd64.tar.gz && \
    tar -xvf gh_2.31.0_linux_amd64.tar.gz && \
    rm gh_2.31.0_linux_amd64.tar.gz && \
    mv gh_2.31.0_linux_amd64/bin/gh /usr/local/bin 
#Install GCloud CLI and Kubectl
RUN curl -OL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-471.0.0-linux-x86_64.tar.gz && \
    tar -xvf google-cloud-sdk-471.0.0-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-471.0.0-linux-x86_64.tar.gz && \
    mv google-cloud-sdk /usr/local/google-cloud-sdk && \
    /usr/local/google-cloud-sdk/install.sh --quiet && \
    /usr/local/google-cloud-sdk/bin/gcloud components install gke-gcloud-auth-plugin && \
    #revert permission
    chown -R runner:runner /home/runner/.config
ENV USE_GKE_GCLOUD_AUTH_PLUGIN=True
ENV PATH="${PATH}:/usr/local/google-cloud-sdk/bin"
#Install Kubectl
RUN curl -OL https://dl.k8s.io/release/v1.28.1/bin/linux/amd64/kubectl && \
    chmod +x ./kubectl && \
    mv ./kubectl /usr/local/bin/kubectl
#Install Apache Maven
RUN curl -OL https://dlcdn.apache.org/maven/maven-3/3.9.4/binaries/apache-maven-3.9.4-bin.tar.gz && \
    tar -xvf apache-maven-3.9.4-bin.tar.gz && \
    rm apache-maven-3.9.4-bin.tar.gz && \
    mv apache-maven-3.9.4 /usr/local/maven
ENV PATH="${PATH}:/usr/local/maven/bin"
ENV MAVEN_HOME="/usr/local/maven"

# Needed to transfer path addtitions to runner environment
RUN echo PATH=$PATH >> /runnertmp/.env
RUN echo USE_GKE_GCLOUD_AUTH_PLUGIN=$USE_GKE_GCLOUD_AUTH_PLUGIN >> /runnertmp/.env
USER runner
