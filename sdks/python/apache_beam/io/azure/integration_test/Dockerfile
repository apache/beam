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

# This image contains a Python SDK build and dependencies.
# By default it runs wordcount against a locally accessible Azurite service.
# See azure_integration_test.sh for example usage.
ARG BASE_IMAGE
FROM $BASE_IMAGE

# Install Azure CLI.
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

WORKDIR /app

# Add Beam SDK sources.
COPY sdks/python /app/sdks/python
COPY model /app/model

# This step should look like setupVirtualenv minus virtualenv creation.
RUN pip install --no-cache-dir tox -r sdks/python/build-requirements.txt

# Add Azurite's self-signed cert to the global CA cert store.
COPY cert.pem /usr/local/share/ca-certificates/azurite.crt
RUN update-ca-certificates

# Tell python to use the global CA cert store.
# See https://stackoverflow.com/a/66494735
ENV REQUESTS_CA_BUNDLE /etc/ssl/certs/ca-certificates.crt

# Run wordcount, and write results to Azurite.
CMD cd sdks/python && tox -e azure_integration_test
