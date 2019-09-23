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

# This script will run python3 ready style checks
#
#   Currently only flake8 E999
#
# The exit-code of the script indicates success or a failure.

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ $PWD != *sdks/python ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi

set -o errexit
set -o pipefail

MODULE=apache_beam

PYTHON_MINOR=$(python -c 'import sys; print(sys.version_info[1])')
if [[ "${PYTHON_MINOR}" == 5 ]]; then
  EXCLUDED_PY3_FILES=$(find ${MODULE} | grep 'py3[6-9]\.py$')
  echo -e "Excluding Py3 files:\n${EXCLUDED_PY3_FILES}"
else
  EXCLUDED_PY3_FILES=""
fi

FILES_TO_IGNORE=""
for file in ${EXCLUDED_PY3_FILES}; do
  if test -z "$FILES_TO_IGNORE"
    then FILES_TO_IGNORE="$(basename $file)"
    else FILES_TO_IGNORE="$FILES_TO_IGNORE, $(basename $file)"
  fi
done

echo -e "Skipping lint for files:\n${FILES_TO_IGNORE}"

usage(){ echo "Usage: $0 [MODULE|--help]  # The default MODULE is $MODULE"; }

if test $# -gt 0; then
  case "$@" in
    --help) usage; exit 1;;
	 *)      MODULE="$*";;
  esac
fi

echo "Running flake8 for module $MODULE:"
flake8 $MODULE --count --select=E9,F821,F822,F823 --show-source --statistics \
  --exclude="${FILES_TO_IGNORE}"
