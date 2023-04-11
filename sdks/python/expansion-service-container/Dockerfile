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
FROM python:3.8-bullseye
MAINTAINER "Apache Beam <dev@beam.apache.org>"

WORKDIR /opt/apache/beam

# Copy the Python source tarball.
COPY target/apache-beam-sdk.tar.gz /opt/apache/beam/

# Copy the requirements file
COPY target/requirements.txt /opt/apache/beam/

# Copy the boot program
COPY target/launcher/linux_amd64/boot /opt/apache/beam/

# Copy the config file
COPY target/expansion_service_config.yml ./

# Creating a virtual environment and installing Beam.
# We do these steps in the Dockefile instead of the boot.go to minimize the
# startup time for the Python expansion service container.
RUN mkdir /opt/apache/beam/beam_venv &&\
    python3 -m venv /opt/apache/beam/beam_venv &&\
    . /opt/apache/beam/beam_venv/bin/activate &&\
    pip install --upgrade pip &&\
    pip install --upgrade setuptools &&\
    pip install -r requirements.txt &&\
    pip install apache-beam-sdk.tar.gz[gcp,dataframe]

ENTRYPOINT ["/opt/apache/beam/boot", "-config_file", "./expansion_service_config.yml"]
