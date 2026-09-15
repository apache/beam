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

# 26.06 ships TensorRT 11.0.0, CUDA 13.3 and Ubuntu 24.04 with Python 3.12.
# TensorRT 8.x cannot target Blackwell GPUs such as the RTX Pro 6000 that
# Dataflow now offers, so this image tracks the current TensorRT major version.
# The Python version here must match the Beam SDK image copied in below.
ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:26.06-py3
ARG BEAM_SDK_IMAGE=apache/beam_python3.12_sdk:latest

FROM ${BEAM_SDK_IMAGE} AS beam_sdk

FROM ${BUILD_IMAGE}

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

COPY --from=beam_sdk /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip \
    && pip install torch>=1.7.1 \
    && pip install torchvision>=0.8.2 \
    && pip install pillow>=8.0.0 \
    && pip install transformers>=4.18.0 \
    && pip install cuda-python

ENTRYPOINT [ "/opt/apache/beam/boot" ]
RUN apt-get update && apt-get install -y python3.12-venv
