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

ARG py_version

FROM python:"${py_version}"-bookworm as python-base

ENV CLOUDSDK_CORE_DISABLE_PROMPTS yes
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY target/base_image_requirements.txt /tmp/base_image_requirements.txt
COPY target/apache-beam.tar.gz /opt/apache/beam/tars/

# Use one RUN command to reduce the number of layers.
RUN  \
    # Install native bindings required for dependencies.
    apt-get update && \
    apt-get install -y \
       # Required by python-snappy
       libsnappy-dev \
       # Required by pyyaml (for c bindings)
       libyaml-dev \
       # This is used to speed up the re-installation of the sdk.
       ccache \
       # Required for using Beam Python SDK on ARM machines.
       libgeos-dev \
       && \
    rm -rf /var/lib/apt/lists/*

RUN python -m venv venv
ENV PATH="/venv/bin:$PATH"
RUN \
    pip install --upgrade setuptools && \

    # Install required packages for Beam Python SDK and common dependencies used by users.
    # use --no-deps to ensure the list includes all transitive dependencies.
    pip install --no-deps -r /tmp/base_image_requirements.txt && \
    rm -rf /tmp/base_image_requirements.txt && \
    python -c "import nltk; nltk.download('stopwords')" && \
    rm /root/nltk_data/corpora/stopwords.zip && \

    # Check that the protobuf upb(also called micro protobuf) is used.
    python -c "from google.protobuf.internal import api_implementation; assert api_implementation._implementation_type == 'upb'; print ('Verified fast protobuf used.')" && \

    # Install Google Cloud SDK.
    mkdir -p /usr/local/gcloud && \
    cd /usr/local/gcloud && \
    curl -s -O https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz && \
    tar -xf google-cloud-sdk.tar.gz && \
    /usr/local/gcloud/google-cloud-sdk/install.sh && \
    rm -rf /usr/local/gcloud/google-cloud-sdk/.install/.backup && \
    rm google-cloud-sdk.tar.gz && \

    # Configure ccache prior to installing Beam SDK. This speeds up wheels compilation when installing the SDK from sources.
    ln -s /usr/bin/ccache /usr/local/bin/gcc && \
    # These parameters are needed as pip compiles artifacts in random temporary directories.
    ccache --set-config=sloppiness=file_macro && ccache --set-config=hash_dir=false && \

    # Install Apache Beam SDK. Use --no-deps and pip check to verify that all
    # necessary dependencies are specified in base_image_requirements.txt.
    pip install --no-deps -v /opt/apache/beam/tars/apache-beam.tar.gz[gcp] && \
    pip check || (echo "Container does not include required Beam dependencies or has conflicting dependencies. If Beam dependencies have changed, you need to regenerate base_image_requirements.txt files. See: https://s.apache.org/beam-python-requirements-generate" && exit 1) && \

    # Log complete list of what exact packages and versions are installed.
    pip freeze --all && \

    # Remove pip cache.
    rm -rf /root/.cache/pip

# extract the deb packages
RUN cd /tmp && \
    # Install native bindings required for dependencies.
    apt-get update && \
    apt-get download \
       # Required by python-snappy
       libsnappy-dev \
       # Required by pyyaml (for c bindings)
       libyaml-dev \
       # This is used to speed up the re-installation of the sdk.
       ccache \
       # Required for using Beam Python SDK on ARM machines.
       libgeos-dev \
       && \
    mkdir /dpkg && \
    for deb in *.deb; do dpkg -i $deb; dpkg --extract $deb /dpkg || exit 10; done

FROM gcr.io/distroless/cc-debian12 as beam
LABEL Author "Apache Beam <dev@beam.apache.org>"
ARG TARGETOS
ARG TARGETARCH

# copy some commands
COPY --from=python-base /bin /bin

ARG  CHIPSET_ARCH=x86_64-linux-gnu
# arm
# ARG  CHIPSET_ARCH=aarch64-linux-gnu

# copy python to distroless
COPY --from=python-base /usr/local/lib/ /usr/local/lib/
COPY --from=python-base /usr/local/bin/python /usr/local/bin/python
COPY --from=python-base /usr/local/bin/pip /usr/local/bin/pip
COPY --from=python-base /etc/ld.so.cache /etc/ld.so.cache
COPY --from=python-base /usr/local/gcloud /usr/local/gcloud


# copy venv
COPY --from=python-base /venv /venv
ENV PATH="/venv/bin:$PATH"

# required by lots of packages - e.g. six, numpy, wsgi
COPY --from=python-base /lib/${CHIPSET_ARCH} /lib/${CHIPSET_ARCH}

# COPY --from=python-base /lib/${CHIPSET_ARCH}/libz* /lib/${CHIPSET_ARCH}/
# COPY --from=python-base /lib/${CHIPSET_ARCH}/libselinux* /lib/${CHIPSET_ARCH}/
# # required by google-cloud/grpcio
# COPY --from=python-base /usr/lib/${CHIPSET_ARCH}/libffi* /usr/lib/${CHIPSET_ARCH}/
# COPY --from=python-base /lib/${CHIPSET_ARCH}/libexpat* /lib/${CHIPSET_ARCH}/

# quick validation that python works
RUN python --version

COPY target/base_image_requirements.txt /tmp/base_image_requirements.txt
COPY target/apache-beam.tar.gz /opt/apache/beam/tars/
COPY target/launcher/${TARGETOS}_${TARGETARCH}/boot target/LICENSE target/NOTICE target/LICENSE.python /opt/apache/beam/

ENV CLOUDSDK_CORE_DISABLE_PROMPTS yes
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# copy all packages over
COPY --from=python-base /dpkg /


ENTRYPOINT ["/opt/apache/beam/boot"]

####
# Pull and add third party licenses to the image if pull_licenses is true.
# Use multistage build to eliminate unwanted changes to beam image due to
# extra dependencies needed to pull licenses.
####

FROM beam as third_party_licenses
ARG pull_licenses
COPY target/license_scripts /tmp/license_scripts/

# Add golang licenses.
COPY  target/go-licenses/* /opt/apache/beam/third_party_licenses/golang/

COPY target/license_scripts /tmp/license_scripts/
RUN if [ "$pull_licenses" = "true" ] ; then \
      pip install 'pip-licenses<5' pyyaml tenacity && \
      python /tmp/license_scripts/pull_licenses_py.py ; \
    fi

FROM beam
ARG pull_licenses
COPY --from=third_party_licenses /opt/apache/beam/third_party_licenses /opt/apache/beam/third_party_licenses
RUN if [ "$pull_licenses" != "true" ] ; then \
      rm -rf /opt/apache/beam/third_party_licenses ; \
    fi
