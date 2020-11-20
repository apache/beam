# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM openjdk:8-jre-alpine

ENV PATH $PATH:/hbase/bin

ARG HBASE_VERSION=2.3.3

WORKDIR /

RUN apk add --no-cache bash

RUN set -eux && \
    apk add --no-cache curl tar && \
    url="https://downloads.apache.org/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \
    curl "$url" -O && \
    mkdir "hbase-$HBASE_VERSION" && \
    tar zxf "hbase-$HBASE_VERSION-bin.tar.gz" -C "hbase-$HBASE_VERSION" --strip 1 && \
    test -d "hbase-$HBASE_VERSION" && \
    rm -fv "hbase-$HBASE_VERSION-bin.tar.gz" && \
    rm -rf "hbase-$HBASE_VERSION/docs" && \
    rm -rf "hbase-$HBASE_VERSION/src" && \
    mkdir /hbase-data && \
    apk del tar curl

VOLUME /hbase-data

EXPOSE 2181 8080 8085 9090 9095 16000 16010 16030 16201 16301

RUN printf "#!/bin/bash \
\n/hbase-2.3.3/bin/start-hbase.sh\n \
\ntail -f /dev/null\n" > /entry.sh

RUN /bin/bash -c 'chmod 777 entry.sh'

ENTRYPOINT ["/entry.sh"]
