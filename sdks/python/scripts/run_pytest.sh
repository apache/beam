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
# Utility script for tox.ini for running unit tests.
#
# Runs tests in parallel, except those not compatible with xdist. Combines
# exit statuses of runs, special-casing 5, which says that no tests were
# selected.
#
# $1 - suite base name
# $2 - additional arguments not parsed by tox (typically module names or
#   '-k keyword')
# $3 - optional arguments to pytest

envname=${1?First argument required: suite base name}
posargs=$2
pytest_args=$3

# Run with pytest-xdist and without.
pytest -o junit_suite_name=${envname} \
  --junitxml=pytest_${envname}.xml -m 'not no_xdist' -n 6 ${pytest_args} --pyargs ${posargs}
status1=$?
pytest -o junit_suite_name=${envname}_no_xdist \
  --junitxml=pytest_${envname}_no_xdist.xml -m 'no_xdist' ${pytest_args} --pyargs ${posargs}
status2=$?

# Exit with error if no tests were run (status code 5).
if [[ $status1 == 5 && $status2 == 5 ]]; then
  exit $status1
fi

# Exit with error if one of the statuses has an error that's not 5.
if [[ $status1 && $status1 != 5 ]]; then
  exit $status1
fi
if [[ $status2 && $status2 != 5 ]]; then
  exit $status2
fi
