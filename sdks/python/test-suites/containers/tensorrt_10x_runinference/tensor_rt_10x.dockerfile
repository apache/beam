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

ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:25.01-py3

FROM ${BUILD_IMAGE}

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

RUN apt-get update -y && \
    apt-get install -y \
    python3-venv \
    libx11-6 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgl1

RUN pip install --no-cache-dir apache-beam[gcp]==2.67.0

COPY --from=apache/beam_python3.10_sdk:2.67.0 /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip && \
    pip install torch && \
    pip install torchvision && \
    pip install pillow>=8.0.0 && \
    pip install transformers>=4.18.0 && \
    pip install cuda-python==12.8 && \
    pip install opencv-python==4.7.0.72 && \
    pip install PyMuPDF==1.22.5 && \
    pip install requests && \
    pip install numpy==2.0.1

ENTRYPOINT ["/opt/apache/beam/boot"]
