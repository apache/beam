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
if [[ $# < 1 || $# > 2 ]]; then
  printf "Usage: \n$> ./scripts/run_tox.sh <tox_environment> [<sdk_location>]"
  printf "\n\ttox_environment: [required] Tox environment to run the test in.\n"
  printf "\n\tsdk_location: [optional] SDK tarball artifact location.\n"
  exit 1
fi

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ $PWD != *sdks/python ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi

# Used in tox.ini to isolate toxworkdir of each environment.
export ENV_NAME=.tox-$1
# Force colors in Jenkins jobs.
if [[ "$JENKINS_HOME" != "" ]]; then
  export PY_COLORS=1
fi

if [[ ! -z $2 ]]; then
  tox -c tox.ini --recreate -e $1 --installpkg $2
else
  tox -c tox.ini --recreate -e $1
fi

exit_code=$?
# Retry once for the specific exit code 245.
if [[ $exit_code == 245 ]]; then
  tox -c tox.ini --recreate -e $1
  exit_code=$?
fi
exit $exit_code
