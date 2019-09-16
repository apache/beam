#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

# This script builds a Docker container with the user specified requirements on top of
# an existing Python worker Docker container (either one you build from source as
# described in CONTAINERS.md or from a released Docker container).

# Quit on any errors
set -e

echo "To add requirements you will need a requirements.txt (you can specify with
 the env variable USER_REQUIREMENTS) and somewhere to push the resulting docker
 image (e.g bintrary, GCP container registry)."

# Be really verbose about each command we are running
set -x


USER_REQUIREMENTS=${USER_REQUIREMENTS:-requirements.txt}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
BASE_PYTHON_IMAGE=${BASE_PYTHON_IMAGE:-"$(whoami)-docker-apache.bintray.io/beam/python"}
NEW_PYTHON_IMAGE=${NEW_PYTHON_IMAGE:-"${BASE_PYTHON_IMAGE}-with-requirements"}
DOCKER_FILE="${SCRIPT_DIR}/../container/extra_requirements/Dockerfile"
TEMP_DIR=$(mktemp -d -t "boo-loves-beam-XXXXXXXXXXXXXXX")
cp $DOCKER_FILE $TEMP_DIR
cp $USER_REQUIREMENTS $TEMP_DIR/requirements.txt
pushd $TEMP_DIR
docker build . -t $NEW_PYTHON_IMAGE --build-arg BASE_PYTHON_IMAGE=$BASE_PYTHON_IMAGE
popd
rm -rf $TEMP_DIR
