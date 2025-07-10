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
ARG BASE_IMAGE=openjdk:11
FROM golang:1-bullseye AS build
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

# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -ldflags="-X main.BuildCommitHash=$GIT_COMMIT -X main.BuildCommitTimestamp=$GIT_TIMESTAMP" -o /go/bin/server_scio_backend

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
RUN chmod +x /opt/playground/backend/new_scio_project.sh

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list &&\
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list &&\
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get update && apt-get install -y sbt

COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /

# Install Kafka emulator
COPY kafka-emulator/kafka-emulator.tar /opt/playground/backend/kafka-emulator/
RUN cd /opt/playground/backend/kafka-emulator/ && tar -xvf kafka-emulator.tar && rm kafka-emulator.tar &&\
    mv kafka-emulator/*.jar . && rmdir kafka-emulator/ &&\
    mv beam-playground-kafka-emulator-*.jar beam-playground-kafka-emulator.jar

# Create a user group `appgroup` and a user `appuser`
RUN groupadd --gid 20000 appgroup \
  && useradd --uid 20000 --gid appgroup --shell /bin/bash --create-home appuser

RUN mkdir -p /opt/playground/backend/executable_files/

# Chown all required files to the `appuser`.
RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ && chmod +x /entrypoint.sh

# Let sbt download files from Maven
RUN mkdir -p /opt/sbt-template
RUN chown -R appuser:appgroup /opt/sbt-template

#Download spotify g8 template at specific commit
ARG g8_commit=7c1ba7c1651dfd70976028842e721da4107c0d6d
RUN wget https://codeload.github.com/spotify/scio.g8/zip/$g8_commit -O scio.g8.zip && unzip scio.g8.zip && mv scio.g8-$g8_commit /opt/scio.g8

# Switch to appuser
USER appuser

# Cache sbt project template
WORKDIR /opt/sbt-template
RUN /opt/playground/backend/new_scio_project.sh
WORKDIR /opt/sbt-template/scio
RUN sbt "+compile"
#chmod is a fix for sbt scala output permission issue that is known in scala-js https://github.com/scala-js/scala-js/issues/4212
RUN chmod -R 0644 /opt/sbt-template/scio/target/scala-*/zinc/*.zip
WORKDIR /

USER root
RUN chown -R root:root /opt/sbt-template

USER appuser

ENV SBT_OPTS="-Xmx512M -XX:+UseG1GC -XX:+UseStringDeduplication"

ENTRYPOINT ["/entrypoint.sh"]
