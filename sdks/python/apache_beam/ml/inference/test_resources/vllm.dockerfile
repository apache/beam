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
# Dockerfile — Beam dev harness + install dev SDK from LOCAL source package

FROM nvidia/cuda:12.4.1-devel-ubuntu22.04

# 1) Non-interactive + timezone
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      tzdata \
      software-properties-common \
      python3.10-full \
      python3.10-distutils \
      build-essential \
      python3.10-dev \
      cython3 && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata && \
    rm -rf /var/lib/apt/lists/*

# 2) Symlink python3 to 3.10
RUN ln -sf /usr/bin/python3.10 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.10 /usr/bin/python

# 3) Install pip, setuptools & wheel
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3 && \
    python3 -m pip install --upgrade pip setuptools wheel

# 4) Copy the Beam SDK harness (for Dataflow workers)
COPY --from=gcr.io/apache-beam-testing/beam-sdk/beam_python3.10_sdk:2.68.0.dev \
     /opt/apache/beam /opt/apache/beam

# 5) Make sure the harness is discovered first
ENV PYTHONPATH=/opt/apache/beam:$PYTHONPATH

# 6) Install the Beam dev SDK from the local source package.
# This .tar.gz file will be created by GitHub Actions workflow
# and copied into the build context.
COPY ./sdks/python/build/apache-beam.tar.gz /tmp/beam.tar.gz
RUN python3 -m pip install --no-cache-dir "/tmp/beam.tar.gz[gcp]"

# 7) Install vLLM, and other dependencies
RUN python3 -m pip install --no-cache-dir \
      openai>=1.52.2 \
      vllm>=0.6.3 \
      triton>=3.1.0

# 8) Use the Beam boot script as entrypoint
ENTRYPOINT ["/opt/apache/beam/boot"]