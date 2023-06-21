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
ARG GO_BASE_IMAGE=golang:1.18-bullseye
ARG SDK_TAG
ARG BASE_IMAGE=apache/beam_python3.10_sdk:$SDK_TAG
FROM $GO_BASE_IMAGE AS build
ARG GIT_COMMIT="<unknown>"
ARG GIT_TIMESTAMP="0"

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Prepare Application
COPY src /go/src/playground/backend
WORKDIR /go/src/playground/backend
RUN ls

# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -ldflags="-X main.BuildCommitHash=$GIT_COMMIT -X main.BuildCommitTimestamp=$GIT_TIMESTAMP" -o /go/bin/server_python_backend

FROM python:3.10-slim-bullseye

ARG SDK_TAG
ARG DOCKER_CHANNEL=stable
ARG DOCKER_VERSION=20.10.24

RUN apt-get update && apt-get install -y --no-install-recommends \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg2 \
  software-properties-common

RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg

RUN echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" ${DOCKER_CHANNEL}" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null

RUN apt-get update && apt-get install -y --no-install-recommends docker-ce=5:${DOCKER_VERSION}~3-0~debian-bullseye && \
  docker -v && \
  dockerd -v

ARG DIND_COMMIT=deda3d4933d3c0bd57f2cef672da5d28fc653706
ENV DOCKER_EXTRA_OPTS '--storage-driver=overlay'
RUN curl -fL -o /usr/local/bin/dind "https://raw.githubusercontent.com/moby/moby/${DIND_COMMIT}/hack/dind" && \
	chmod +x /usr/local/bin/dind

ENV VIRTUAL_ENV=/opt/playground/venv
RUN mkdir -p $VIRTUAL_ENV
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ARG BEAM_VERSION
RUN pip install --upgrade pip
RUN pip install apache-beam[gcp,interactive,dataframe]==${SDK_TAG}
RUN pip install --default-timeout=100 torch
RUN pip install torchvision
RUN pip install pandas
RUN pip install scikit-learn

RUN apt-get update
RUN apt-get install -y wget

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -
RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN apt-get update
RUN apt-get update && apt-get install -y openjdk-11-jre-headless

ARG BEAM_VERSION
ARG REPO_URL=https://repo1.maven.org/maven2

ARG MULTILANGUAGE_PIPELINE_EXAMPLES=${REPO_URL}/org/apache/beam/beam-examples-multi-language/${SDK_TAG}/beam-examples-multi-language-${SDK_TAG}.jar
RUN wget  $MULTILANGUAGE_PIPELINE_EXAMPLES &&\
mv beam-examples-multi-language-${SDK_TAG}.jar beam-examples-multi-language.jar

################
## Pre-pulled images ##
################
COPY ./images/* /images/

ARG SDK_TAG
ENV BEAM_VERSION=$SDK_TAG
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_PYTHON"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml
ENV JOB_SERVICE_PORT=9091
ENV EXT_SERVICE_PORT=9092 

# Copy build result
COPY --from=build /go/bin/server_python_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/

# Install Python Katas Utils
ENV PYTHONPATH="$PYTHONPATH:/go/src/katas"

COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /

# Install Kafka emulator
COPY kafka-emulator/kafka-emulator.tar /opt/playground/backend/kafka-emulator/
RUN cd /opt/playground/backend/kafka-emulator/ && tar -xvf kafka-emulator.tar && rm kafka-emulator.tar &&\
    mv kafka-emulator/*.jar . && rmdir kafka-emulator/ &&\
    mv beam-playground-kafka-emulator-*.jar beam-playground-kafka-emulator.jar
RUN apt-get update && apt-get install -y openjdk-11-jre-headless

# Create a user group `appgroup` and a user `appuser`
RUN groupadd --gid 20000 appgroup \
  && useradd --uid 20000 --gid appgroup --shell /bin/bash --create-home appuser

RUN mkdir -p /opt/playground/backend/executable_files/
RUN mkdir -p /home/appuser/.apache_beam/cache/jars/

ARG SQL_EXPANSION_SERVICE_JAR=${REPO_URL}/org/apache/beam/beam-sdks-java-extensions-sql-expansion-service/${SDK_TAG}/beam-sdks-java-extensions-sql-expansion-service-${SDK_TAG}.jar
RUN wget -O /home/appuser/.apache_beam/cache/jars/beam-sdks-java-extensions-sql-expansion-service.jar $SQL_EXPANSION_SERVICE_JAR

# Chown all required files to the `appuser`.
RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ && chmod +x /entrypoint.sh

VOLUME /var/lib/docker

ENTRYPOINT ["/entrypoint.sh"]
