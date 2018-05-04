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

FROM openjdk:8-jdk
MAINTAINER Beam dev@beam.apache.org

# Install dependencies for Jenkins Python tests
RUN apt-get update && \
    apt-get install -y \
      python-pip \
      python-virtualenv \
      python-dev \
      python-tox \
      maven \
      rsync \
      time \
  && rm -rf /var/lib/apt/lists/*

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz -O gcloud.tar.gz && \
tar xf gcloud.tar.gz && \
./google-cloud-sdk/install.sh --quiet
ENV PATH="/google-cloud-sdk/bin:${PATH}"
RUN gcloud components update --quiet || echo 'gcloud components update failed'


# Create beam user to validate the build on user space
ENV USER=user \
    UID=1001 \
    GID=1002 \
    HOME=/home/user
RUN groupadd --system --gid=$GID $USER; \
    useradd --system --uid=$UID --gid $USER $USER;
RUN mkdir -p $HOME; \
    chown -R $USER:$USER $HOME; \
    chmod 777 $HOME;
USER $USER
WORKDIR $HOME

# Add the entrypoint script which install gcloud sdk
#COPY docker-entrypoint.sh /usr/local/bin/
#RUN ln -s usr/local/bin/docker-entrypoint.sh /entrypoint.sh
#ENTRYPOINT ["docker-entrypoint.sh"]

#ARG URL=https://github.com/apache/beam
#
#RUN git clone $URL beam; \
#  cd beam; \
#  git config --local --add remote.origin.fetch '+refs/pull/*/head:refs/remotes/origin/pr/*'; \
#  git fetch --quiet --all;
