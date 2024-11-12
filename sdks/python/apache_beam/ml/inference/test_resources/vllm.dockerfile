# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Used for any vLLM integration test

FROM nvidia/cuda:12.4.1-devel-ubuntu22.04

RUN apt update
RUN apt install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt update

ARG DEBIAN_FRONTEND=noninteractive

RUN apt install python3.12 -y
RUN apt install python3.12-venv -y
RUN apt install python3.12-dev -y
RUN rm /usr/bin/python3
RUN ln -s python3.12 /usr/bin/python3
RUN python3 --version
RUN apt-get install -y curl
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12 && pip install --upgrade pip

RUN pip install --no-cache-dir -vvv apache-beam[gcp]==2.58.1
RUN pip install openai vllm

RUN apt install libcairo2-dev pkg-config python3-dev -y
RUN pip install pycairo

# Copy the Apache Beam worker dependencies from the Beam Python 3.12 SDK image.
COPY --from=apache/beam_python3.12_sdk:2.58.1 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]
