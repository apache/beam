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
FROM python:"${py_version}"-bookworm as beam
LABEL Author "Apache Beam <dev@beam.apache.org>"
ARG TARGETOS
ARG TARGETARCH

COPY target/base_image_requirements.txt /tmp/base_image_requirements.txt
COPY target/apache-beam.tar.gz /opt/apache/beam/tars/
COPY target/launcher/${TARGETOS}_${TARGETARCH}/boot target/LICENSE target/NOTICE target/LICENSE.python /opt/apache/beam/

ENV CLOUDSDK_CORE_DISABLE_PROMPTS yes
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

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
    rm -rf /var/lib/apt/lists/* && \

    pip install --upgrade pip setuptools wheel && \

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

FROM beam as base
ARG pull_licenses
COPY --from=third_party_licenses /opt/apache/beam/third_party_licenses /opt/apache/beam/third_party_licenses
RUN if [ "$pull_licenses" != "true" ] ; then \
      rm -rf /opt/apache/beam/third_party_licenses ; \
    fi

ARG TARGETARCH
FROM gcr.io/distroless/python3-debian12:latest-${TARGETARCH} as distroless
ARG py_version

# Contains header files needed by the Python interpreter.
COPY --from=base /usr/local/include /usr/local/include

# Contains the Python interpreter executables.
COPY --from=base /usr/local/bin /usr/local/bin

# Contains the Python library dependencies.
COPY --from=base /usr/local/lib /usr/local/lib

# Contains the boot entrypoint and related files such as licenses.
COPY --from=base /opt /opt

ENV PATH "$PATH:/usr/local/bin"

# Despite the ENTRYPOINT set above, need to reset since deriving the layer derives from a different image.
ENTRYPOINT ["/opt/apache/beam/boot"]
