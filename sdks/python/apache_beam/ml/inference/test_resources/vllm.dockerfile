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

# Copy the Apache Beam worker dependencies from the Beam Python 3.8 SDK image.
COPY --from=apache/beam_python3.12_sdk:2.58.1 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]
