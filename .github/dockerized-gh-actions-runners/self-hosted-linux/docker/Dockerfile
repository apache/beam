################################################################################
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
################################################################################

FROM ubuntu:20.04

ARG RUNNER_VERSION=2.286.0

RUN apt-get -yqq update
#Install APT packages
RUN DEBIAN_FRONTEND=noninteractive apt-get install -yqq acl aria2 apt-transport-https binutils bison brotli build-essential bzip2 ca-certificates coreutils curl dbus dnsutils dpkg fakeroot file flex fonts-noto-color-emoji ftp gnupg2 haveged imagemagick iproute2 iputils-ping jq lib32z1 libc++-dev libc++abi-dev libcurl4 libgbm-dev libgconf-2-4 libgsl-dev libgtk-3-0 libicu66 libkrb5-3 liblttng-ust0 libmagic-dev libmagickcore-dev libmagickwand-dev libsecret-1-dev libssl1.1 libsqlite3-dev libxss1 locales m4 mediainfo mercurial net-tools netcat openssh-client p7zip-full p7zip-rar parallel pass patchelf pkg-config pollinate python-is-python3 rpm rsync shellcheck sqlite3 ssh sshpass subversion sudo swig telnet texinfo time tk tzdata unzip upx wget xorriso xvfb xz-utils zip zlib1g zsync 
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
RUN useradd -m actions && usermod -aG sudo actions && echo "%sudo ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER actions
WORKDIR /home/actions

RUN curl -O -L https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-x64-$RUNNER_VERSION.tar.gz
RUN tar xzf ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz

COPY --chown=actions:actions entrypoint.sh ./entrypoint.sh

RUN sudo chmod u+x ./entrypoint.sh
RUN curl -fsSL https://get.docker.com -o get-docker.sh
RUN sudo sh get-docker.sh

CMD ["/home/actions/entrypoint.sh"]
ENTRYPOINT ["/bin/bash"]
