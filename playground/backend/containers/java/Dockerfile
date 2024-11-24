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
ARG BEAM_VERSION=2.44.0
FROM golang:1-bullseye AS build
ARG BEAM_VERSION
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
#COPY playground /go/src/playground/playground
WORKDIR /go/src/playground/backend

# Build Application
RUN go mod download &&\
    go mod tidy &&\
    cd cmd/server &&\
    go build -ldflags="-X main.BuildCommitHash=$GIT_COMMIT -X main.BuildCommitTimestamp=$GIT_TIMESTAMP" -o /go/bin/server_java_backend

FROM maven:3.8.6-openjdk-11 as dep
ARG BEAM_VERSION
RUN mvn archetype:generate \
    -DinteractiveMode=false \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-gcp-bom-examples \
    -DarchetypeVersion=$BEAM_VERSION \
    -DtargetPlatform=11 \
    -DartifactId=pipeline-dependencies \
    -DgroupId=org.apache.beam.samples
WORKDIR /pipeline-dependencies/
RUN mvn dependency:resolve && mvn dependency:go-offline &&  mvn -o dependency:list
RUN mvn dependency:copy-dependencies

FROM apache/beam_java11_sdk:$BEAM_VERSION

ARG BEAM_VERSION
ARG SPRING_VERSION=5.3.27
ARG KAFKA_CLIENTS_VERSION=2.3.1
ENV BEAM_VERSION=$BEAM_VERSION
ENV SERVER_IP=0.0.0.0
ENV SERVER_PORT=8080
ENV APP_WORK_DIR=/opt/playground/backend/
ENV BEAM_SDK="SDK_JAVA"
ENV PROPERTY_PATH=/opt/playground/backend/properties.yaml
ARG CALCITE_VERSION=1_28_0
ARG BYTEBUDDY_VERSION=1.12.14
ARG JANINO_VERSION=3.0.11

# Copy build result
COPY --from=build /go/bin/server_java_backend /opt/playground/backend/
COPY --from=build /go/src/playground/backend/configs /opt/playground/backend/configs/
COPY --from=build /go/src/playground/backend/logging.properties /opt/playground/backend/
COPY --from=build /go/src/playground/backend/datasets /opt/playground/backend/datasets/
COPY --from=dep /pipeline-dependencies/target/dependency/ /opt/apache/beam/jars/

# Install Beam Examples
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-examples-java/$BEAM_VERSION/beam-examples-java-$BEAM_VERSION.jar &&\
    mv beam-examples-java-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-examples-java.jar
# Install jars for Playground graphs
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-core/$BEAM_VERSION/beam-sdks-java-core-$BEAM_VERSION-tests.jar &&\
    mv beam-sdks-java-core-$BEAM_VERSION-tests.jar /opt/apache/beam/jars/beam-sdks-java-core-tests.jar
# Install Spring Expression
RUN wget https://repo1.maven.org/maven2/org/springframework/spring-expression/$SPRING_VERSION/spring-expression-$SPRING_VERSION.jar &&\
    mv spring-expression-$SPRING_VERSION.jar /opt/apache/beam/jars/spring-expression.jar
# Install Spring Core
RUN wget https://repo1.maven.org/maven2/org/springframework/spring-core/$SPRING_VERSION/spring-core-$SPRING_VERSION.jar &&\
    mv spring-core-$SPRING_VERSION.jar /opt/apache/beam/jars/spring-core.jar
# Install Spring JCL
RUN wget https://repo1.maven.org/maven2/org/springframework/spring-jcl/$SPRING_VERSION/spring-jcl-$SPRING_VERSION.jar &&\
    mv spring-jcl-$SPRING_VERSION.jar /opt/apache/beam/jars/spring-jcl.jar

# Install sql extensions service and dependencies for Beam
RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-extensions-sql/$BEAM_VERSION/beam-sdks-java-extensions-sql-$BEAM_VERSION.jar &&\
    mv beam-sdks-java-extensions-sql-$BEAM_VERSION.jar /opt/apache/beam/jars/beam-sdks-java-extensions-sql.jar

RUN wget https://repo1.maven.org/maven2/org/apache/beam/beam-vendor-calcite-$CALCITE_VERSION/0.2/beam-vendor-calcite-$CALCITE_VERSION-0.2.jar &&\
    mv beam-vendor-calcite-$CALCITE_VERSION-0.2.jar /opt/apache/beam/jars/beam-vendor-calcite-$CALCITE_VERSION.jar

RUN wget  https://repo1.maven.org/maven2/net/bytebuddy/byte-buddy/$BYTEBUDDY_VERSION/byte-buddy-$BYTEBUDDY_VERSION.jar &&\
    mv byte-buddy-$BYTEBUDDY_VERSION.jar /opt/apache/beam/jars/byte-buddy-$BYTEBUDDY_VERSION.jar

RUN wget  https://repo1.maven.org/maven2/org/codehaus/janino/janino/$JANINO_VERSION/janino-$JANINO_VERSION.jar &&\
    mv janino-$JANINO_VERSION.jar /opt/apache/beam/jars/janino-$JANINO_VERSION.jar

RUN wget  https://repo1.maven.org/maven2/org/codehaus/janino/commons-compiler/$JANINO_VERSION/commons-compiler-$JANINO_VERSION.jar &&\
    mv commons-compiler-$JANINO_VERSION.jar /opt/apache/beam/jars/commons-compiler-$JANINO_VERSION.jar
    
# Install Java Katas Utils
COPY katas /go/src/katas
RUN cd /go/src/katas &&\
    ./gradlew jar &&\
    mv util/build/libs/util.jar /opt/apache/beam/jars/util.jar

COPY src/properties.yaml /opt/playground/backend/properties.yaml
COPY entrypoint.sh /
COPY kafka-emulator/kafka-emulator.tar /opt/playground/backend/kafka-emulator/
RUN cd /opt/playground/backend/kafka-emulator/ && tar -xvf kafka-emulator.tar && rm kafka-emulator.tar &&\
    mv kafka-emulator/*.jar . && rmdir kafka-emulator/ &&\
    mv beam-playground-kafka-emulator-*.jar beam-playground-kafka-emulator.jar

# Create a user group `appgroup` and a user `appuser`
RUN groupadd --gid 20000 appgroup \
  && useradd --uid 20000 --gid appgroup --shell /bin/bash --create-home appuser

RUN mkdir -p /opt/playground/backend/executable_files/

# Chown all required files to the `appuser`.

RUN chown -R appuser:appgroup /opt/playground/backend/executable_files/ && chmod o+r /opt/apache/beam/jars/* \
  && chmod +x /entrypoint.sh

# Switch to appuser
USER appuser

ENTRYPOINT ["/entrypoint.sh"]
