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

usage(){ echo "Usage: $0 [MODULE|--help] # The default MODULE is $MODULE"; }

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
echo "Skipping lint for generated files: $FILES_TO_IGNORE"

echo "Running pylint --py3k for modules $( printf "%s " "${MODULE}" ):"
pylint -j8 $( printf "%s " "${MODULE}" ) \
  --ignore-patterns="$FILES_TO_IGNORE" --py3k
