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

set -e

# The files are gathered by the archiveFilesToLint then unpackFilesToLint tasks.
# Currently all *.md (markdown) files and build.gradle (gradle build) files in
# the Beam repo are included.
pushd ../../../files-to-whitespacelint

declare -a failed_files

# Check for trailing spaces in non-code text files.
echo "Running whitespacelint..."
while IFS= read -r -d '' file; do
  if ! test -f "$file"; then
    echo "$file does not exist"
    continue
  fi
  if ! whitespacelint "$file"; then
    failed_files+=("$file")
  fi
done < <(find . -type f -print0)

if [ ${#failed_files[@]} -gt 0 ]; then
  printf '=%.0s' {1..100}
  echo
  echo "Whitespacelint has found above issues."
  echo "You can open the file with vim and remove trailing whitespaces by:"
  for file in "${failed_files[@]}"; do
    echo "vim -es -c \":%s/\s\+$//e\" -c \":wq\" \"$file\""
  done
  printf '=%.0s' {1..100}
  echo
  popd
  exit 1
else
  printf '=%.0s' {1..100}
  echo
  echo 'Whitespacelint has completed successfully and found no issue.'
  printf '=%.0s' {1..100}
  echo
  popd
fi
