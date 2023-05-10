#!/bin/bash
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# Usage
# ./generate_build_metadata.sh <build_commit_hash> <build_commit_seconds_since_epoch>

FILE=$(realpath "$(dirname "$0")/../lib/src/build_metadata.g.dart")

cat > "$FILE" << EOF
// GENERATED CODE - DO NOT MODIFY BY HAND
//
// This file is generated during deployment to contain data about the commit.
// The copy of this file stored in the repository is for development purpose
// so the project can be started locally.
// Keep these constants \`null\` in the repository.

const buildCommitHash = '$1';
const buildCommitSecondsSinceEpoch = $2;
EOF

echo "Written $FILE:"
cat "$FILE"
