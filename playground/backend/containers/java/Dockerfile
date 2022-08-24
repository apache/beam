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
FROM golang:1.18-bullseye AS build

# Setup Go Environment
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1 &&\
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0

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
ARG BEAM_VERSION=2.40.0
ARG HAMCREST_VERSION=1.3
ARG JUNIT_VERSION=4.13.2
ARG BEAM_VENDOR_GRPC_VERSION=0.1
#ENV BEAM_VERSION=${BEAM_VERSION_ARG}
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_JAVA"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml

# Copy build result
COPY --from=build /go/bin/server_java_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/
COPY --from=build /go/src/playground/backend/logging.properties /opt/playground/backend/

# Install Beam DirectRunner
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-runners-direct-java/$BEAM_VERSION/beam-runners-direct-java-$BEAM_VERSION.jar &&\
    mv beam-runners-direct-java-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-runners-direct.jar
# Install Beam SDK Core
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/$BEAM_VERSION/beam-sdks-java-core-$BEAM_VERSION.jar &&\
    mv beam-sdks-java-core-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-sdks-java-core.jar
# Imstall Beam Examples
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-examples-java/$BEAM_VERSION/beam-examples-java-$BEAM_VERSION.jar &&\
    mv beam-examples-java-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-examples-java.jar
# Install Beam SDK Core tests
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/$BEAM_VERSION/beam-sdks-java-core-$BEAM_VERSION-tests.jar &&\
    mv beam-sdks-java-core-$BEAM_VERSION-tests.jar /opt/apache/beam/jars/beam-sdks-java-core-tests.jar
# Install Hamcrest
RUN wget https://repo1.maven.org/maven2/org/hamcrest/hamcrest-all/$HAMCREST_VERSION/hamcrest-all-$HAMCREST_VERSION.jar &&\
    mv hamcrest-all-$HAMCREST_VERSION.jar /opt/apache/beam/jars/hamcrest-all.jar
# Install JUnit
RUN wget https://repo1.maven.org/maven2/junit/junit/$JUNIT_VERSION/junit-$JUNIT_VERSION.jar &&\
    mv junit-$JUNIT_VERSION.jar /opt/apache/beam/jars/junit.jar
# Install Beam Vendor gRPC
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-vendor-grpc-1_26_0/$BEAM_VENDOR_GRPC_VERSION/beam-vendor-grpc-1_26_0-$BEAM_VENDOR_GRPC_VERSION.jar &&\
    mv beam-vendor-grpc-1_26_0-$BEAM_VENDOR_GRPC_VERSION.jar /opt/apache/beam/jars/beam-vendor-grpc.jar
# Install Java Katas Utils
COPY katas /go/src/katas
RUN cd /go/src/katas &&\
    ./gradlew jar &&\
    mv util/build/libs/util.jar /opt/apache/beam/jars/util.jar

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
RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ && chmod -R 775 /opt/apache/beam/jars/ \
  && chmod -R 777 /usr/local/share/ca-certificates/extra/ && chmod -R 777 /usr/local/openjdk-8/jre/lib/security/ \
  && chmod -R 777 /etc/ssl/certs && chmod +x /entrypoint.sh

# Switch to appuser
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
