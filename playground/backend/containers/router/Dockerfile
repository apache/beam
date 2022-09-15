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
ARG BASE_IMAGE=golang:1.18-bullseye
#Two-stage assembly
FROM $BASE_IMAGE AS build

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Prepare Application
COPY src /go/src/playground/backend

ARG BEAM_VERSION=2.40.0
ENV PREPARED_MOD_DIR=/opt/playground/prepared_folder/
ENV PIPELINES_FOLDER_NAME="executable_files"
RUN mkdir -p /opt/playground/ $PREPARED_MOD_DIR
WORKDIR $PREPARED_MOD_DIR

RUN go mod init $PIPELINES_FOLDER_NAME &&\
    go get -u github.com/apache/beam/sdks/v2@v$BEAM_VERSION &&\
    go mod download all

# Build Application

#RUN
WORKDIR /go/src/playground/backend
RUN ls
# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -o /go/bin/server_go_backend

# Null image
FROM debian:stretch-slim

# Install deps being used by sh files
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Environment
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_UNSPECIFIED"
ENV BUCKET_NAME="playground-precompiled-objects"
ENV SDK_CONFIG=/opt/playground/backend/sdks.yaml
ENV PROPERTY_PATH=/opt/playground/backend/

# Copy build result
COPY --from=build /go/bin/server_go_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/

COPY sdks.yaml /opt/playground/backend/sdks.yaml
COPY sdks-emulator.yaml /opt/playground/backend/sdks-emulator.yaml
COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
