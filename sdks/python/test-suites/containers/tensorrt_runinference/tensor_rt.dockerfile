ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:22.05-py3

FROM ${BUILD_IMAGE} 

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

RUN pip install --no-cache-dir apache-beam[gcp]==2.40.0
COPY --from=apache/beam_python3.8_sdk:2.40.0 /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip \
    && pip install torch>=1.7.1 \
    && pip install torchvision>=0.8.2 \
    && pip install pillow>=8.0.0 \
    && pip install transformers>=4.18.0 \
    && pip install cuda-python

ENTRYPOINT [ "/opt/apache/beam/boot" ]
