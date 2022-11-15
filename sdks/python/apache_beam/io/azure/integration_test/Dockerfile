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
# By default it runs wordcount against a locally accessible HDFS service.
# See azure_integration_test.sh for example usage.
ARG BASE_IMAGE
FROM $BASE_IMAGE

RUN apt-get install -y ca-certificates

WORKDIR /app

# Add Beam SDK sources.
COPY sdks/python /app/sdks/python
COPY model /app/model

RUN cp /app/sdks/python/apache_beam/io/azure/integration_test/cert.pem /usr/local/share/ca-certificates
RUN update-ca-certificates

# This step should look like setupVirtualenv minus virtualenv creation.
RUN pip install --no-cache-dir tox==3.11.1 -r sdks/python/build-requirements.txt

# Run wordcount, and write results to Azure.
CMD cd sdks/python && tox -e azure_integration_test
