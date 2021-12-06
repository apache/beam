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
ARG BASE_IMAGE=apache/beam_java8_sdk:latest
FROM golang:1.17-buster AS build

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1

# Prepare Application
COPY src /go/src/playground/backend
#COPY playground /go/src/playground/playground
WORKDIR /go/src/playground/backend
RUN ls
# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -o /go/bin/server_java_backend

FROM $BASE_IMAGE
ARG BEAM_VERSION=2.33.0
#ENV BEAM_VERSION=${BEAM_VERSION_ARG}
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_JAVA"

# Copy build result
COPY --from=build /go/bin/server_java_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/
COPY --from=build /go/src/playground/backend/logging.properties /opt/playground/backend/

# Install Beam DirectRunner
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-runners-direct-java/$BEAM_VERSION/beam-runners-direct-java-$BEAM_VERSION.jar &&\
    mv beam-runners-direct-java-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-runners-direct.jar
RUN printenv
ENTRYPOINT ["/opt/playground/backend/server_java_backend"]
