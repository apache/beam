#!/bin/bash
set -e
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

# Get this script's absolute path
SCRIPT_PATH=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Extract release notes from the blog post
POST_PATH=$(echo "${SCRIPT_PATH}/../../../../website/www/site/content/en/blog/beam-${RELEASE_VER}.md")
RELEASE_NOTES=$(
    cat ${POST_PATH} |                               # Read post's content
    sed -n '/<!--/,$p' |                             # Remove post's metadata
    sed -e :a -Ee 's/<!--.*-->.*//g;/<!--/N;//ba' |  # Remove license
    sed '/./,$!d'                                    # Remove leading whitespace
)

# Escape notes' content to work with JSON
ESCAPED_NOTES=$(printf '%s' "${RELEASE_NOTES}" | python -c 'import json,sys; print(json.dumps(sys.stdin.read()))')

# Build JSON for the API request
REQUEST_JSON="$(cat <<-EOF
{
  "tag_name": "v${RELEASE_VER}",
  "name": "Beam ${RELEASE_VER} release",
  "body": ${ESCAPED_NOTES}
}
EOF
)"

## Send request to Github API
curl https://api.github.com/repos/apache/beam/releases \
-X POST \
-H "Authorization: token ${GITHUB_TOKEN}" \
-H "Content-Type:application/json" \
-d "${REQUEST_JSON}"

echo -e "\nView the release on Github: https://github.com/apache/beam/releases/tag/v${RELEASE_VER}"