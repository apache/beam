#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

MY_DIR="$(cd "$(dirname "$0")" && pwd)"
pushd "${MY_DIR}" &>/dev/null || exit 1

echo "Working directory: ${MY_DIR}"

PROJECT_ROOT_REL_PATH=../..

DIST_DIR=${1:-"./site/code_samples"}
echo "Dist directory: ${DIST_DIR}"

CONTENT_DIR=${2:-"./site/content"}

mapfile -t code_sample_uris < <(grep -rh "{{< code_sample" "${CONTENT_DIR}" | sed -e 's/^.*"\(.*\)".*$/\1/g' | sort | uniq | xargs -n 1 echo)

mkdir -pv "${DIST_DIR}"

for uri in "${code_sample_uris[@]}"
do
  fileName=$(echo "$uri" | sed -e 's/\//_/g')
  cp "$PROJECT_ROOT_REL_PATH"/"$uri" "$DIST_DIR"/"$fileName"
done

popd &>/dev/null || exit 1
