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

# This script will be run by Jenkins as a post commit test. In order to run
# locally make the following changes:
#
# LOCAL_PATH   -> Path of tox and virtualenv if you have them already installed.
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for service jobs.
#
# Execute from the root of the repository: sdks/python/run_postcommit.sh

set -e
set -v

# pip install --user installation location.
export LOCAL_PATH=$HOME/.local/bin/

# Remove any tox cache from previous workspace
rm -rf sdks/python/target/.tox

# INFRA does not install virtualenv
pip install virtualenv --user

# INFRA does not install tox
pip install tox --user

# Tox runs unit tests in a virtual environment
${LOCAL_PATH}/tox -e ALL -c sdks/python/tox.ini

python post_commit.py
