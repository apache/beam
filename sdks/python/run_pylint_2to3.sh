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

# This script will run pylint with the --py3k parameter to check for python
# 3 compatibility. This script can run on a list of modules provided as
# command line arguments.
#
# The exit-code of the script indicates success or a failure.

set -o errexit
set -o pipefail

DEFAULT_MODULE=apache_beam

usage(){ echo "Usage: $0 [MODULE|--help]
# The default MODULE is $DEFAULT_MODULE"; }

MODULES=${DEFAULT_MODULE}
while [[ $# -gt 0 ]] ; do
  key="$1"
  case ${key} in
    --help) usage; exit 1;;
	*)
	if [ ${MODULES} = ${DEFAULT_MODULE} ] ; then
	  MODULES=()
	fi
	MODULES+=("$1")
	shift;;
  esac
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

echo "Running pylint --py3k for modules $( printf "%s " "${MODULES[@]}" ):"
pylint -j8 $( printf "%s " "${MODULES[@]}" ) \
  --ignore-patterns="$FILES_TO_IGNORE" --py3k