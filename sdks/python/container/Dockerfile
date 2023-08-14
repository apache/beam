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

ARG py_version
FROM python:"${py_version}"-bullseye as beam
LABEL Author "Apache Beam <dev@beam.apache.org>"
ARG TARGETOS
ARG TARGETARCH

COPY target/base_image_requirements.txt /tmp/base_image_requirements.txt
COPY target/apache-beam.tar.gz /opt/apache/beam/tars/
COPY target/launcher/${TARGETOS}_${TARGETARCH}/boot target/LICENSE target/NOTICE target/LICENSE.python /opt/apache/beam/

ENV CLOUDSDK_CORE_DISABLE_PROMPTS yes
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# Use one RUN command to reduce the number of layers.
RUN  \
    # Install native bindings required for dependencies.
    pip freeze --all && \

    # Remove pip cache.
    rm -rf /root/.cache/pip

ENTRYPOINT ["/opt/apache/beam/boot"]

####
# Pull and add third party licenses to the image if pull_licenses is true.
# Use multistage build to eliminate unwanted changes to beam image due to
# extra dependencies needed to pull licenses.
####

FROM beam as third_party_licenses
ARG pull_licenses
COPY target/license_scripts /tmp/license_scripts/

# Add golang licenses.
COPY  target/go-licenses/* /opt/apache/beam/third_party_licenses/golang/

COPY target/license_scripts /tmp/license_scripts/
RUN if [ "$pull_licenses" = "true" ] ; then \
      pip install 'pip-licenses<5' pyyaml tenacity && \
      python /tmp/license_scripts/pull_licenses_py.py ; \
    fi

FROM beam
ARG pull_licenses
COPY --from=third_party_licenses /opt/apache/beam/third_party_licenses /opt/apache/beam/third_party_licenses
RUN if [ "$pull_licenses" != "true" ] ; then \
      rm -rf /opt/apache/beam/third_party_licenses ; \
    fi
