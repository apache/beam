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

# strip leading/trailing quotes from posargs because it can get double quoted as its passed through.
posargs=$(sed -e 's/^"//' -e 's/"$//' -e "s/'$//" -e "s/^'//" <<<$posargs)
echo "pytest_args: $pytest_args"
echo "posargs: $posargs"

# Define the regex for extracting the -m argument value
marker_regex="-m\s+('[^']+'|\"[^\"]+\"|[^ ]+)"

# Initialize the user_marker variable.
user_marker=""

# Isolate the user-provided -m argument (matching only).
if [[ $pytest_args =~ "-m" ]]; then
  # Extract the marker value using the defined regex.
  user_marker=$(echo "$pytest_args" | sed -nE "s/.*$marker_regex.*/\1/p")
fi

# Remove the -m argument from pytest_args (substitution only).
if [[ -n $user_marker ]]; then
  pytest_args=$(echo "$pytest_args" | sed -E "s/$marker_regex//")
fi

# Combine user-provided marker with script's internal logic.
marker_for_parallel_tests="not no_xdist"
marker_for_sequential_tests="no_xdist"

if [[ -n $user_marker ]]; then
  # Combine user marker with internal markers.
  marker_for_parallel_tests="($user_marker) and ($marker_for_parallel_tests)"
  marker_for_sequential_tests="($user_marker) and ($marker_for_sequential_tests)"
fi

# Run tests in parallel.
echo "Running parallel tests with: pytest -m \"$marker_for_parallel_tests\" $pytest_args"
pytest -v -rs -o junit_suite_name=${envname} \
  --junitxml=pytest_${envname}.xml -m "$marker_for_parallel_tests" -n 6 --import-mode=importlib ${pytest_args} --pyargs ${posargs}
status1=$?

# Run tests sequentially.
echo "Running sequential tests with: pytest -m \"$marker_for_sequential_tests\" $pytest_args"
pytest -v -rs -o junit_suite_name=${envname}_no_xdist \
  --junitxml=pytest_${envname}_no_xdist.xml -m "$marker_for_sequential_tests" --import-mode=importlib ${pytest_args} --pyargs ${posargs}
status2=$?

# Exit with error if no tests were run in either suite (status code 5).
if [[ $status1 == 5 && $status2 == 5 ]]; then
  exit $status1
fi

# Exit with error if one of the statuses has an error that's not 5.
if [[ $status1 != 0 && $status1 != 5 ]]; then
  exit $status1
fi
if [[ $status2 != 0 && $status2 != 5 ]]; then
  exit $status2
fi
