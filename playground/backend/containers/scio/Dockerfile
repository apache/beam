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
ARG BASE_IMAGE=openjdk:8
FROM golang:1.18-bullseye AS build

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

# Prepare Application
COPY src /go/src/playground/backend
WORKDIR /go/src/playground/backend

# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -o /go/bin/server_scio_backend

FROM $BASE_IMAGE
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_SCIO"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml

# Copy build result
COPY --from=build /go/bin/server_scio_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/
COPY --from=build /go/src/playground/backend/logging.properties /opt/playground/backend/
COPY --from=build /go/src/playground/backend/new_scio_project.sh /opt/playground/backend/
COPY --from=build /go/src/playground/backend/internal/fs_tool/ExampleData.scala /opt/playground/backend/

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get update && apt-get install -y sbt


## Install mitmpoxy
RUN mkdir /opt/mitmproxy &&\
    cd /opt/mitmproxy &&\
    wget https://snapshots.mitmproxy.org/7.0.4/mitmproxy-7.0.4-linux.tar.gz &&\
    tar -zxvf mitmproxy-7.0.4-linux.tar.gz &&\
    mkdir /usr/local/share/ca-certificates/extra
COPY allow_list_proxy.py /opt/mitmproxy/
COPY allow_list.py /opt/mitmproxy/
ENV HTTP_PROXY="http://127.0.0.1:8081"
ENV HTTPS_PROXY="http://127.0.0.1:8081"

COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /

# Create a user group `appgroup` and a user `appuser`
RUN groupadd --gid 20000 appgroup \
  && useradd --uid 20000 --gid appgroup --shell /bin/bash --create-home appuser

RUN mkdir -p /opt/playground/backend/executable_files/

# Chown all required files to the `appuser`.
RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ \
  && chmod -R 777 /usr/local/share/ca-certificates/extra/ && chmod -R 777 /usr/local/openjdk-8/jre/lib/security/ \
  && chmod -R 777 /etc/ssl/certs && chmod +x /entrypoint.sh

# Switch to appuser
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
