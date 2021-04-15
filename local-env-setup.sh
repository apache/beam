#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

darwin_install_pip3_packages() {
    echo "Installing setuptools grpcio-tools virtualenv"
    pip3 install setuptools grpcio-tools virtualenv
    echo "Installing mypy-protobuf"
    pip3 install --user mypy-protobuf
}

install_go_packages(){
        echo "Installing goavro"
        go get github.com/linkedin/goavro
        # As we are using bash, we are assuming .bashrc exists. 
        grep -qxF "export GOPATH='${pwd}/sdks/go/examples/.gogradle/project_gopath'" ~/.bashrc || echo "export GOPATH='${pwd}/sdks/go/examples/.gogradle/project_gopath'" >> ~/.bashrc
        # The following source command will not work if executed in a subshell like $ ./this_script.sh. Instead, we need to run it in the current shell as $ . ./this_script.sh.
        # However, this is just to load the GOPATH env variable from the previosu command into the current shell. If that's not required, then we can suggest a terminal reload after the execution?
        #source ~/.bashrc
}

# Running on Linux
if [ "$(uname -s)" = "Linux" ]; then
    # Assuming Debian based Linux and the prerequisites in https://beam.apache.org/contribute/ are met:
    apt-get update

    echo "Installing openjdk-8-jdk, python-setuptools, python3-pip, tox, docker.io"
    apt-get install -y openjdk-8-jdk python-setuptools python3-pip tox docker.io # using python3-pip as Python3 is required.

    type -P python3 > /dev/null 2>&1
    python3Exists=$?
    type -P pip3 > /dev/null 2>&1
    pip3Exists=$?
    if [ $python3Exists -eq 0  -a $pip3Exists -eq 0 ]; then
        echo "Installing grpcio-tools and mypy-protobuf"
        pip3 install grpcio-tools mypy-protobuf virtualenv
    else
        echo "Python3 and pip3 are required. Installation Failed."
        exit
    fi

    type -P go > /dev/null 2>&1
    goExists=$?
    if [ $goExists -eq 0 ]; then
        install_go_packages
    else
        echo "Go is required. Installation Failed."
        exit
    fi
fi

# Running on Mac
if [ "$(uname -s)" = "Darwin" ]; then
    # Check for Homebrew, install if we don't have it
    type -P brew > /dev/null 2>&1
    brewExists=$?
    if [ $brewExists -ne 0 ]; then
        echo "Installing homebrew"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi

    # Update homebrew recipes
    echo "Updating brew"
    brew update

    # Assuming we are using brew
    if brew ls --versions openjdk@8 > /dev/null; then
        echo "openjdk@8 already installed. Skipping"
    else
        echo "Installing openjdk@8"
        brew install openjdk@8
    fi

    type -P python3 > /dev/null 2>&1
    python3Exists=$?
    type -P pip3 > /dev/null 2>&1
    pip3Exists=$?
    if [ $python3Exists -eq 0  -a $pip3Exists -eq 0 ]; then
        darwin_install_pip3_packages
    else
        echo "Python3 and pip3 are required. Installation Failed."
        exit
    fi

    type -P tox > /dev/null 2>&1
    toxExists=$?
    if [ $toxExists -eq 0 ]; then
        echo "tox already installed. Skipping"
    else
        echo "Installing tox"
        brew install tox
    fi

    type -P docker > /dev/null 2>&1
    dockerExists=$?
    if [ $dockerExists -eq 0 ]; then
        echo "docker already installed. Skipping"
    else
        echo "Installing docker"
        brew install --cask docker
    fi

    type -P go > /dev/null 2>&1
    goExists=$?
    if [ $goExists -eq 0 ]; then
        install_go_packages
    else
        echo "Go is required. Installation Failed."
        exit
    fi
fi