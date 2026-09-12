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
ARG base_image
ARG java_version
FROM ${base_image}:${java_version}
LABEL Author "Apache Beam <dev@beam.apache.org>"
ARG TARGETOS
ARG TARGETARCH

ARG pull_licenses

# Dependency jars
COPY target/slf4j-api.jar \
     target/slf4j-jdk14.jar \
     target/jcl-over-slf4j.jar \
     target/log4j-over-slf4j.jar \
     target/log4j-to-slf4j.jar \
     # Required to use jamm as a javaagent to get accurate object size measuring
     target/jamm.jar \
     /opt/apache/beam/jars/

# Built jars
COPY target/open-module-agent.jar target/beam-sdks-java-harness.jar /opt/apache/beam/jars/

# Built binary with licenses
COPY target/${TARGETOS}_${TARGETARCH}/boot target/LICENSE target/NOTICE /opt/apache/beam/

# copy third party licenses
ADD target/third_party_licenses /opt/apache/beam/third_party_licenses/

# Copy Java options. Because the options directory may be empty and
# COPY fails if there are no files, copy an extra LICENSE file then remove it.
COPY target/LICENSE target/options/* /opt/apache/beam/options/

# Add golang licenses.
COPY target/go-licenses/* /opt/apache/beam/third_party_licenses/golang/
RUN if [ "${pull_licenses}" = "false" ] ; then \
    # Remove above license dir if pull licenses false
    rm -rf /opt/apache/beam/third_party_licenses ; \
   fi

# Add Google Cloud Profiler agent.
COPY target/profiler/* /opt/google_cloud_profiler/

ENTRYPOINT ["/opt/apache/beam/boot"]
