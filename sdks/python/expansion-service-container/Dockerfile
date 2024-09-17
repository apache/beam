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

# We just need to support one Python version supported by Beam.
# Picking the current default Beam Python version which is Python 3.8.
FROM python:3.9-bookworm as expansion-service
LABEL Author "Apache Beam <dev@beam.apache.org>"
ARG TARGETOS
ARG TARGETARCH

WORKDIR /opt/apache/beam

# Copy the Python source tarball.
COPY target/apache-beam-sdk.tar.gz /opt/apache/beam/

# Copy the requirements file
COPY target/requirements.txt /opt/apache/beam/

# Copy the boot program
COPY target/launcher/${TARGETOS}_${TARGETARCH}/boot /opt/apache/beam/

# Creating a virtual environment and installing Beam.
# We do these steps in the Dockefile instead of the boot.go to minimize the
# startup time for the Python expansion service container.
RUN mkdir /opt/apache/beam/beam_venv &&\
    python3 -m venv /opt/apache/beam/beam_venv &&\
    . /opt/apache/beam/beam_venv/bin/activate &&\
    pip install --upgrade pip &&\
    pip install --upgrade setuptools &&\
    pip install apache-beam-sdk.tar.gz[gcp,dataframe] --constraint requirements.txt

ENTRYPOINT ["/opt/apache/beam/boot"]

####
# Pull and add third party licenses to the image if pull_licenses is true.
# Use multistage build to eliminate unwanted changes to beam image due to
# extra dependencies needed to pull licenses.
####

FROM expansion-service as third_party_licenses
ARG pull_licenses
COPY target/license_scripts /tmp/license_scripts/

# Add golang licenses.
COPY  target/go-licenses/* /opt/apache/beam/third_party_licenses/golang/

COPY target/license_scripts /tmp/license_scripts/
RUN if [ "$pull_licenses" = "true" ] ; then \
      pip install 'pip-licenses<5' pyyaml tenacity && \
      python /tmp/license_scripts/pull_licenses_py.py ; \
    fi

FROM expansion-service
ARG pull_licenses
COPY --from=third_party_licenses /opt/apache/beam/third_party_licenses /opt/apache/beam/third_party_licenses
RUN if [ "$pull_licenses" != "true" ] ; then \
      rm -rf /opt/apache/beam/third_party_licenses ; \
    fi
