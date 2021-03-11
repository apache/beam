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
# Utility script for tox.ini for running validates runner tests via pytest.

# Usage check.
if [[ $# < 2 ]]; then
  printf "Wrong number of arguments (found $#, expected 2+). Usage:\n"
  printf "$> ./scripts/pytest_validates_runner.sh <envname> <testFile> <posarg>\n"
  printf "\tenvname: suite base name.\n"
  printf "\ttestFile: the file to test.\n"
  printf "\tAdditional options will be passed as --test-pipeline-options.\n"
  exit 1
fi

set -e

envname=${1?First argument required: suite base name}
shift;
testFile=${1?Second argument required: test file path}
shift;

# Assume test pipeline options are double-escaped.
pytest -o junit_suite_name=${envname} --junitxml=pytest_${envname}.xml ${testFile} "--test-pipeline-options=$@"
