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

ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:22.05-py3

FROM ${BUILD_IMAGE} 

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

RUN pip install --no-cache-dir apache-beam[gcp]==2.48.0
COPY --from=apache/beam_python3.8_sdk:2.48.0 /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip \
    && pip install torch>=1.7.1 \
    && pip install torchvision>=0.8.2 \
    && pip install pillow>=8.0.0 \
    && pip install transformers>=4.18.0 \
    && pip install cuda-python

ENTRYPOINT [ "/opt/apache/beam/boot" ]
RUN apt-get update && apt-get install -y python3.8-venv 
