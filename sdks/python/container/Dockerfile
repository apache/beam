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

# Install packages required by the Python SDK.
#
# These packages should be kept in sync with the dependencies at
# sdks/python/setup.py.  If not installed, sdk harness will install these at
# runtime, but we would like to avoid doing this so that we do not depend on
# PyPI at runtime whenever possible.
#
# Also install cython, numpy, pandas and scipy as well as their dependencies.
# These are standard Python packages, likely to be used by python SDK customers,
# and their dependencies.
#
RUN \
    # These are packages needed by the Python SDK.
    pip install "avro == 1.8.2" && \
    pip install "crcmod == 1.7" && \
    pip install "dill == 0.2.6" && \
    pip install "grpcio == 1.3.0" && \
    pip install "httplib2 == 0.9.2" && \
    pip install "mock == 2.0.0" && \
    pip install "oauth2client == 3.0.0" && \
    pip install "protobuf == 3.3.0" && \
    pip install "pyyaml == 3.12" && \
    pip install "pyvcf == 0.6.8" && \
    pip install "six == 1.10.0" && \
    pip install "typing == 3.6.1" && \
    pip install "futures == 3.1.1" && \
    # Setup packages
    pip install "nose == 1.3.7" && \
    # GCP extra features
    pip install "google-apitools == 0.5.11" && \
    pip install "proto-google-cloud-datastore-v1 == 0.90.4" && \
    pip install "googledatastore == 7.0.1" && \
    pip install "google-cloud-pubsub == 0.26.0" && \
    pip install "google-cloud-bigquery == 0.25.0" && \
    # Optional packages
    pip install "cython == 0.27.2" && \
    pip install "guppy == 0.1.10" && \
    pip install "python-snappy == 0.5.1" && \
    # These are additional packages likely to be used by customers.
    pip install "numpy == 1.13.3" --no-binary=:all: && \
    pip install "pandas == 0.18.1" && \
    pip install "scipy == 1.0.0" && \
    pip install "protobuf == 3.3.0" && \
    pip install "tensorflow == 1.4.0" && \
    pip install "protorpc == 0.11.1" && \
    pip install "python-gflags == 3.0.6" && \
    # Remove pip cache.
    rm -rf /root/.cache/pip && \
    # Check that the fast implementation of protobuf is used.
    python -c "from google.protobuf.internal import api_implementation; assert api_implementation._default_implementation_type == 'cpp'; print 'Verified fast protobuf used.'"

COPY target/apache-beam.tar.gz /opt/apache/beam/tars/
RUN pip install /opt/apache/beam/tars/apache-beam.tar.gz[gcp]

ADD target/linux_amd64/boot /opt/apache/beam/

ENTRYPOINT ["/opt/apache/beam/boot"]
