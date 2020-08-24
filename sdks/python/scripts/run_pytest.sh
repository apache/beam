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
# Arguments:
# $1 - suite base name
# $2 - additional arguments to pass to pytest
#
# Options:
# --cov         - code directory to report coverage
# --cov-report  - code coverage report format
# -p, --package - package names need to be tested
#
# Example usages:
# - Run all tests and generate coverage report
#   `$ ./run_pytest.sh envname posargs --cov-report xml:codecov.xml --cov apache_beam`
# - Run tests under apache_beam.typehints and apache_beam.options packages
#   `$ ./run_pytest.sh envname posargs -p apache_beam.typehints -p apache_beam.options

test_packages=""

check_argument_existence() {
  if [[ -z "$2" ]] || [[ "$2" =~ ^-+ ]]; then
    echo "Argument is required for this option: $1" 1>&2
    exit 1
  fi
}

for OPT in "$@"
do
  case $OPT in
    -p | --package)
      check_argument_existence "$1" "$2"
      test_packages="${test_packages}${2} "
      shift 2
      ;;
    --cov-report)
      check_argument_existence "$1" "$2"
      coverage_report="--cov-report=${2}"
      shift 2
      ;;
    --cov)
      check_argument_existence "$1" "$2"
      coverage_root="--cov=${2}"
      shift 2
      ;;
    *)
      if [[ ! "$1" =~ ^-+ ]]; then
        if ! ${envname+:} false; then
          envname=$1
        elif ! ${posargs+:} false; then
          posargs=$1
        fi
        shift 1
      fi
      ;;
  esac
done

if [[ -z "${envname}" ]]; then
  echo "First argument required: suite base name" 1>&2
  exit 1
fi

# Run with pytest-xdist and without.
pytest ${test_packages} -o junit_suite_name=${envname} \
  --junitxml=pytest_${envname}.xml -m 'not no_xdist' -n 6 ${coverage_report} ${coverage_root} --pyargs ${posargs}
status1=$?
pytest ${test_packages} -o junit_suite_name=${envname}_no_xdist \
  --junitxml=pytest_${envname}_no_xdist.xml -m 'no_xdist' ${coverage_report} ${coverage_root} --pyargs ${posargs}
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
