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

###########################################################################
#
# This script is a wrapper around tox.
# This is a workaround to the bug related to Nose mentioned in [Beam-5243].
# TODO: [Beam-5243] Remove this wrapper after we migrate from Nose.

###########################################################################
# Usage check.
if [[ $# != 1 ]]; then
  printf "Usage: \n$> ./scripts/run_tox.sh <tox_environment>"
  printf "\n\ttox_environment: [required] Tox environment to run the test in.\n"
  exit 1
fi

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ "*sdks/python" != $PWD ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi

set -v
tox -c tox.ini --recreate -e $1
# Retry once for the specific exit code -11.
if [[ $? == -11 ]]; then
  tox -c tox.ini --recreate -e $1
fi
