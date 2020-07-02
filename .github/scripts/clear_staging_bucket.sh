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
# This script removes folders from GCP_PATH bucket which does not have corresponding branches.
set -e

declare -a FOLDERS_TO_BE_DELETED=()
IFS=$'\n' read -r -d '' -a existing_folders < <((gsutil ls -d "${GCP_PATH}" || return 1) | grep "^${GCP_PATH}" | sed "s?.*${GCP_PATH}/??" | sed -e 's/\/$//' && printf '\0')
IFS=$'\n' read -r -d '' -a existing_branches < <(git ls-remote --heads origin | sed 's?.*refs/heads/??' && printf '\0')

for i in "${existing_folders[@]}"; do
  if [[ ! "${existing_branches[@]}" =~ "${i}" ]]; then
    echo "For folder ${GCP_PATH}/${i} the corresponding branch '${i}' does not exist and it will be deleted"
    FOLDERS_TO_BE_DELETED+=("${i}")
  fi
done

if [ ${#FOLDERS_TO_BE_DELETED[@]} -eq 0 ]; then
  echo "No Folders to be deleted, hooray!"
else
  for i in "${FOLDERS_TO_BE_DELETED[@]}"; do
    echo "Deleting ${GCP_PATH}/${i}/ ..."
    gsutil -m rm "${GCP_PATH}/${i}/**"
  done
fi
