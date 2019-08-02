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

FROM python:2-stretch
MAINTAINER "Apache Beam <dev@beam.apache.org>"

# Install native bindings required for dependencies.
RUN apt-get update && \
    apt-get install -y \
       # These packages are needed for "pip install python-snappy" below.
       libsnappy-dev \
       # This package is needed for "pip install pyyaml" below to have c bindings.
       libyaml-dev \
       && \
    rm -rf /var/lib/apt/lists/*

# Install the Python SDK along with its dependences, as well as common
# dependencies of user code.

COPY target/base_image_requirements.txt /tmp/base_image_requirements.txt
COPY target/apache-beam.tar.gz /opt/apache/beam/tars/
RUN \
    pip install -r /tmp/base_image_requirements.txt && \
    # Check that the fast implementation of protobuf is used.
    python -c "from google.protobuf.internal import api_implementation; assert api_implementation._default_implementation_type == 'cpp'; print ('Verified fast protobuf used.')" && \
    # Remove pip cache.
    rm -rf /root/.cache/pip

ADD target/launcher/linux_amd64/boot /opt/apache/beam/

ENTRYPOINT ["/opt/apache/beam/boot"]
