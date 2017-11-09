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

FROM maven:3-jdk-7

# Download OS dependencies
RUN apt-get update && \
    apt-get install -y \
      libsnappy1 \
      python-pip \
      python-virtualenv \
      python-dev \
      rsync \
  && rm -rf /var/lib/apt/lists/*

# Add the entrypoint script that downloads the beam sources on run
COPY docker-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/docker-entrypoint.sh /entrypoint.sh # backwards compat
ENTRYPOINT ["docker-entrypoint.sh"]

# Create beam user to validate the build on user space
ENV USER=user \
    UID=9999 \
    HOME=/home/user
RUN groupadd --system --gid=$UID $USER; \
    useradd --system --uid=$UID --gid $USER $USER;
RUN mkdir -p $HOME; \
    chown -R $USER:$USER $HOME;
USER $USER
WORKDIR $HOME

ENV URL=https://github.com/apache/beam/archive/master.zip
ENV SRC_FILE=master.zip
ENV SRC_DIR=beam-master
