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
FROM $BASE_IMAGE

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
    go get github.com/apache/beam/sdks/v2/go/pkg/beam@v$BEAM_VERSION &&\
    go get github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs@v$BEAM_VERSION &&\
    go mod download all

# Build Application

#RUN
WORKDIR /go/src/playground/backend
RUN ls
# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -o /opt/playground/backend/server_go_backend

ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_GO"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml
## Copy build result
COPY src/configs /opt/playground/backend/configs/

# Install mitmpoxy
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
  && chmod -R 777 /usr/local/share/ca-certificates/extra/ && chmod -R 777 /etc/ssl/certs && chmod +x /entrypoint.sh

# Switch to appuser
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
