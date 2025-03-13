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

# This script will run pylint and pep8 on all module files.
#
# Use "pylint apache_beam" to run pylint all files.
# Use "pep8 apache_beam" to run pep8 all files.
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

MODULE=$(ls -d -C apache_beam *.py)

usage(){ echo "Usage: $0 [MODULE|--help]  # The default MODULE is $MODULE"; }

if test $# -gt 0; then
  case "$@" in
    --help) usage; exit 1;;
	 *)      MODULE="$*";;
  esac
fi

# Following generated files are excluded from lint checks.
EXCLUDED_GENERATED_FILES=(
"apache_beam/io/gcp/internal/clients/bigquery/bigquery_v2_client.py"
"apache_beam/io/gcp/internal/clients/bigquery/bigquery_v2_messages.py"
"apache_beam/runners/dataflow/internal/clients/dataflow/dataflow_v1b3_client.py"
"apache_beam/runners/dataflow/internal/clients/dataflow/dataflow_v1b3_messages.py"
"apache_beam/io/gcp/internal/clients/storage/storage_v1_client.py"
"apache_beam/io/gcp/internal/clients/storage/storage_v1_messages.py"
"apache_beam/coders/proto2_coder_test_messages_pb2.py"
)

# more portable than shopt -s globstar
while IFS= read -d $'\0' -r file ; do
    EXCLUDED_GENERATED_FILES+=("$file")
done < <(find apache_beam/portability/api -type f -name "*pb2*.py" -print0)

FILES_TO_IGNORE=""
for file in "${EXCLUDED_GENERATED_FILES[@]}"; do
  if test -z "$FILES_TO_IGNORE"
    then FILES_TO_IGNORE="$(basename $file)"
    else FILES_TO_IGNORE="$FILES_TO_IGNORE, $(basename $file)"
  fi
done

echo -e "Skipping lint for files:\n${FILES_TO_IGNORE}"
echo -e "Linting modules:\n${MODULE}"

echo "Running pylint..."
pylint -j8 ${MODULE} --ignore-patterns="$FILES_TO_IGNORE"
echo "Running flake8..."
flake8 ${MODULE} --count --select=E9,F821,F822,F823 --show-source --statistics \
  --exclude="${FILES_TO_IGNORE}"

echo "Running isort..."
# Skip files where isort is behaving weirdly
ISORT_EXCLUDED=(
  "apiclient.py"
  "avroio_test.py"
  "datastore_wordcount.py"
  "datastoreio_test.py"
  "hadoopfilesystem.py"
  "iobase_test.py"
  "fast_coders_test.py"
  "slow_coders_test.py"
  "tfdv_analyze_and_validate.py"
  "preprocess.py"
  "model.py"
  "taxi.py"
  "process_tfma.py"
  "doctests_test.py"
  "render_test.py"
  "yaml/main.py"
  "main_test.py"
)
SKIP_PARAM=""
for file in "${ISORT_EXCLUDED[@]}"; do
  SKIP_PARAM="$SKIP_PARAM --skip $file"
done
for file in "${EXCLUDED_GENERATED_FILES[@]}"; do
  SKIP_PARAM="$SKIP_PARAM --skip $(basename $file)"
done
isort ${MODULE} -p apache_beam --line-width 120 --check-only --order-by-type \
    --combine-star --force-single-line-imports --diff --recursive ${SKIP_PARAM}

echo "Checking unittest.main..."
TESTS_MISSING_MAIN=$(
    find ${MODULE} \
    | grep '\.py$' \
    | xargs grep -l '^import unittest$' \
    | xargs grep -L unittest.main \
    || true)
if [ -n "${TESTS_MISSING_MAIN}" ]; then
  echo -e "\nThe following files are missing a call to unittest.main():"
  for FILE in ${TESTS_MISSING_MAIN}; do
    echo "  ${FILE}"
  done
  echo
  exit 1
fi
