ARG BASE=tensorflow/tensorflow:2.10.0-gpu
FROM $BASE

# Check that the chosen base image provides the expected version of Python interpreter.
ARG PY_VERSION=3.8
RUN [[ $PY_VERSION == `python -c 'import sys; print("%s.%s" % sys.version_info[0:2])'` ]] \
   || { echo "Could not find Python interpreter or Python version is different from ${PY_VERSION}"; exit 1; }

RUN apt-get update -y && apt-get install git -y \
    && apt install libcairo2-dev -y \
    && pip install --user git+https://github.com/pygobject/pycairo.git

RUN pip install --upgrade pip setuptools \
    && pip install scipy \
    && pip install --no-cache-dir apache-beam[gcp]==2.42.0
RUN pip install pillow
    # Verify that there are no conflicting dependencies.
# Copy the Apache Beam worker dependencies from the Beam Python 3.8 SDK image.
COPY --from=apache/beam_python3.8_sdk:2.42.0 /opt/apache/beam /opt/apache/beam

# Apache Beam worker expects pip at /usr/local/bin/pip by default.
# Some images have pip in a different location. If necessary, make a symlink.
# This can be omitted in Beam 2.30.0 and later versions.
RUN [[ `which pip` == "/usr/local/bin/pip" ]] || ln -s `which pip` /usr/local/bin/pip

# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]
