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

FROM openjdk:8
MAINTAINER "Apache Beam <dev@beam.apache.org>"

ARG pull_licenses

ADD target/slf4j-api.jar /opt/apache/beam/jars/
ADD target/slf4j-jdk14.jar /opt/apache/beam/jars/
ADD target/beam-sdks-java-harness.jar /opt/apache/beam/jars/

# Required to run cross-language pipelines with KafkaIO
# TODO May be removed once custom environments are supported
ADD target/beam-sdks-java-io-kafka.jar /opt/apache/beam/jars/
ADD target/kafka-clients.jar /opt/apache/beam/jars/

ADD target/linux_amd64/boot /opt/apache/beam/

COPY target/LICENSE /opt/apache/beam/
COPY target/NOTICE /opt/apache/beam/

# copy third party licenses
ADD target/third_party_licenses /opt/apache/beam/third_party_licenses/

# Add golang licenses. Because the go-license directory may be empty if
# pull_licenses is false, and COPY fails if there are no files,
# copy an extra LICENSE file then remove it.
COPY target/LICENSE target/go-licenses/* /opt/apache/beam/third_party_licenses/golang/
RUN rm /opt/apache/beam/third_party_licenses/golang/LICENSE
RUN if [ "$pull_licenses" = "false" ] ; then \
    # Remove above license dir if pull licenses false
    rm -rf /opt/apache/beam/third_party_licenses ; \
   fi

ENTRYPOINT ["/opt/apache/beam/boot"]
