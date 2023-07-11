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
#Dokerfile to set up the Beam Go SDK
ARG BASE_IMAGE
FROM $BASE_IMAGE
ARG SDK_TAG
ARG SDK_TAG_LOCAL
ARG GIT_COMMIT="<unknown>"
ARG GIT_TIMESTAMP="0"

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" &&\
    chmod -R 777 "$GOPATH/bin"


RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Setup Go SDK dependencies
COPY beam /opt/playground/beam
COPY setup_sdk.sh /

ENV BEAM_SRC /opt/playground/beam
ENV PREPARED_MOD_DIR /opt/playground/prepared_folder
RUN /setup_sdk.sh /opt/playground/prepared_folder

# Prepare Application
COPY src /go/src/playground/backend

# Build Application
WORKDIR /go/src/playground/backend
# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -ldflags="-X main.BuildCommitHash=$GIT_COMMIT -X main.BuildCommitTimestamp=$GIT_TIMESTAMP" -o /opt/playground/backend/server_go_backend

ENV BEAM_VERSION=$SDK_TAG
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_GO"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml
## Copy build result
COPY src/configs /opt/playground/backend/configs/

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

# Chown all required files to the `appuser`.
RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ && chmod +x /entrypoint.sh

# Switch to appuser
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
