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
ARG BASE_IMAGE=golang:1-bullseye
#Two-stage assembly
FROM $BASE_IMAGE AS build
ARG GIT_COMMIT="<unknown>"
ARG GIT_TIMESTAMP="0"

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

# Prepare Application
COPY src /go/src/playground/backend

# Build Application

WORKDIR /go/src/playground/backend
# Build Application
RUN go mod tidy -v &&\
    go mod download &&\
    cd cmd/server &&\
    go build -ldflags="-X main.BuildCommitHash=$GIT_COMMIT -X main.BuildCommitTimestamp=$GIT_TIMESTAMP" -o /go/bin/server_go_backend

# Build migration tool
RUN cd cmd/migration_tool &&\
    go build -o /go/bin/migration_tool

# Null image
FROM debian:stable-20221114-slim

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
ENV SDK_CONFIG=/opt/playground/backend/sdks.yaml
ENV PROPERTY_PATH=/opt/playground/backend/

# Copy build result
COPY --from=build /go/bin/server_go_backend /opt/playground/backend/
COPY --from=build /go/bin/migration_tool /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/

COPY sdks.yaml /opt/playground/backend/sdks.yaml
COPY sdks-emulator.yaml /opt/playground/backend/sdks-emulator.yaml
COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
