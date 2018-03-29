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

set -o errexit
set -o pipefail

DEFAULT_MODULE=apache_beam

usage(){ echo "Usage: $0 [MODULE|--help]
# The default MODULE is $DEFAULT_MODULE"; }

MODULES=${DEFAULT_MODULE}
PY3K=false
while [[ $# -gt 0 ]] ; do
  key="$1"
  case ${key} in
    --help) usage; exit 1;;
    --py3k) PY3K=true; shift;;
	*)
	if [ ${MODULES} = ${DEFAULT_MODULE} ] ; then
	  MODULES=()
	fi
	MODULES+=("$1")
	shift;;
  esac
done

# Following generated files are excluded from lint checks.
EXCLUDED_GENERATED_FILES=(
"apache_beam/io/gcp/internal/clients/bigquery/bigquery_v2_client.py"
"apache_beam/io/gcp/internal/clients/bigquery/bigquery_v2_messages.py"
"apache_beam/runners/dataflow/internal/clients/dataflow/dataflow_v1b3_client.py"
"apache_beam/runners/dataflow/internal/clients/dataflow/dataflow_v1b3_messages.py"
"apache_beam/io/gcp/internal/clients/storage/storage_v1_client.py"
"apache_beam/io/gcp/internal/clients/storage/storage_v1_messages.py"
"apache_beam/coders/proto2_coder_test_messages_pb2.py"
apache_beam/portability/api/*pb2*.py
)

FILES_TO_IGNORE=""
for file in "${EXCLUDED_GENERATED_FILES[@]}"; do
  if test -z "$FILES_TO_IGNORE"
    then FILES_TO_IGNORE="$(basename $file)"
    else FILES_TO_IGNORE="$FILES_TO_IGNORE, $(basename $file)"
  fi
done
echo "Skipping lint for generated files: $FILES_TO_IGNORE"

echo "Running pylint for modules $( printf "%s " "${MODULES[@]}" ):"
pylint -j8 $( printf "%s " "${MODULES[@]}" ) \
  --ignore-patterns="$FILES_TO_IGNORE" \
  $( [ "$PY3K" = true ] && printf %s '--py3k' ) \
  || (echo "Please execute futurize stage 2 to remain python 3 compatible."
  echo
  exit 1)

echo "Running pycodestyle for modules$( printf "%s " "${MODULES[@]}" ):"
pycodestyle $( printf "%s " "${MODULES[@]}" ) --exclude="$FILES_TO_IGNORE"
echo "Running flake8 for modules $( printf "%s " "${MODULES[@]}" ):"
# TODO(BEAM-3959): Add F821 (undefined names) as soon as that test passes
flake8 $( printf "%s " "${MODULES[@]}" ) --count --select=E9,F822,F823 \
  --show-source --statistics

echo "Running isort for modules $( printf "%s " "${MODULES[@]}" ):"
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
  "vcfio.py"
)
SKIP_PARAM=""
for file in "${ISORT_EXCLUDED[@]}"; do
  SKIP_PARAM="$SKIP_PARAM --skip $file"
done
for file in "${EXCLUDED_GENERATED_FILES[@]}"; do
  SKIP_PARAM="$SKIP_PARAM --skip $(basename $file)"
done

for module in "$MODULES"; do
    pushd "$module"
    isort -p apache_beam --line-width 120 --check-only --order-by-type \
      --combine-star --force-single-line-imports --diff ${SKIP_PARAM}
    popd
done

FUTURIZE_EXCLUDED=(
  "typehints.py"
  "pb2"
  "trivial_infernce.py"
)
FUTURIZE_GREP_PARAM=$( IFS='|'; echo "${ids[*]}" )
echo "Checking for files requiring stage 1 refactoring from futurize"
futurize_results=$(futurize -j 8 --stage1 apache_beam 2>&1 |grep Refactored)
futurize_filtered=$(echo "$futurize_results" |grep -v "$FUTURIZE_GREP_PARAM" \
  || echo "")
count=${#futurize_filtered}
if [ "$count" != "0" ]; then
  echo "Some of the changes require futurize stage 1 changes."
  echo "The files with required changes:"
  echo "$futurize_filtered"
  echo "You can run futurize apache_beam to see the proposed changes."
  exit 1
fi
echo "No future changes needed"

echo "Checking unittest.main for modules $( printf "%s " "${MODULES[@]}" ):"
for module in "$MODULES"; do
  TESTS_MISSING_MAIN=$(find ${module} | grep '\.py$' | xargs grep -l \
  '^import unittest$' | xargs grep -L unittest.main)
  if [ -n "${TESTS_MISSING_MAIN}" ]; then
    echo -e "\nThe following files are missing a call to unittest.main():"
    for FILE in ${TESTS_MISSING_MAIN}; do
      echo "  ${FILE}"
    done
    echo
    exit 1
  fi
done