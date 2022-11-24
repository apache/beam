FROM pytorch/pytorch:1.9.1-cuda11.1-cudnn8-runtime


RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/* \
    # Install the pipeline requirements and check that there are no conflicts.
    # Since the image already has all the dependencies installed,
    # there's no need to run with the --requirements_file option.
    && pip install --no-cache-dir --upgrade pip \
    && pip install pillow \
    && pip check
RUN pip install apache_beam[gcp]==2.42.0
# Set the entrypoint to Apache Beam SDK worker launcher.
COPY --from=apache/beam_python3.8_sdk:2.42.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT [ "/opt/apache/beam/boot" ]
