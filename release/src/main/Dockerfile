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

# This builds a container environment capable of building Beam release containers
# through a docker-in-docker configuration. Launch the container with the following:
#
# docker run --privileged -it --entrypoint '/bin/bash' ${tag}:latest
#
# then run `service docker start` and `docker login docker.io` to connect to the docker
# daemon and authenticate with Docker Hub.
FROM debian:bookworm
LABEL Author "Apache Beam <dev@beam.apache.org>"
SHELL ["/bin/bash", "-c"]

RUN apt-get update && \
    apt-get install git zip unzip curl vim wget sudo -y

RUN apt-get install -y build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev llvm \
    libncurses5-dev libncursesw5-dev xz-utils tk-dev \
    libffi-dev liblzma-dev python3-openssl

# Install pyenv and install all of the Python versions
# needed to build containers
RUN curl https://pyenv.run | bash && \
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> /root/.bashrc && \
    echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> /root/.bashrc && \
    echo ''eval "$(pyenv init -)"'' >> /root/.bashrc && \
    source /root/.bashrc && \
    pyenv install 3.9.4 && \
    pyenv install 3.10.7 && \
    pyenv install 3.11.3 && \
    pyenv install 3.12.3 && \
    pyenv global 3.9.4 3.10.7 3.11.3 3.12.3

# Install a Go version >= 1.16 so we can bootstrap higher
# Go versions
RUN wget https://golang.org/dl/go1.23.1.linux-amd64.tar.gz && \
    tar -zxvf go1.23.1.linux-amd64.tar.gz -C /usr/local/ && \
    echo "export PATH=/usr/local/go/bin:${PATH}" | tee /etc/profile.d/go.sh && \
    chmod a+x "/etc/profile.d/go.sh" && \
    source "/etc/profile.d/go.sh"

RUN echo "export PATH=/usr/local/go/bin:${PATH}" >> /root/.bashrc

# Download SDKman for Java version management
RUN curl -s "https://get.sdkman.io" | bash && \
    chmod a+x "/root/.sdkman/bin/sdkman-init.sh"

# Install Java 8 and Java 11
RUN source "/root/.sdkman/bin/sdkman-init.sh" && \
    sdk install java 11.0.14-tem && \
    sdk install java 8.0.322-tem -y

# Set JAVA11_HOME for Java container builds
ENV JAVA11_HOME /root/.sdkman/candidates/java/11.0.14-tem

# Install Node.js and NPM
RUN apt-get install -y nodejs npm && \
    npm install -g n && \
    n lts

# Configure Docker's repository to be pulled from directly
RUN apt-get install -y ca-certificates curl gnupg && \
    sudo install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

RUN echo   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    sudo apt-get update

# Install docker
RUN sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Copy Beam
RUN git clone https://github.com/apache/beam