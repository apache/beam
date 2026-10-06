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

# Sets the version of every crate in the Rust SDK workspace.
#
# Usage: set_version.sh <version>
#
# Called by release/src/main/scripts/set_version.sh with a release version
# (X.Y.Z) or a snapshot version (X.Y.Z-SNAPSHOT). Cargo requires semver, so
# snapshots use a -SNAPSHOT pre-release suffix.
#
# Updates:
#   - [workspace.package] version in Cargo.toml,
#   - the version on every internal path dependency in any Cargo.toml,
#   - workspace member entries in Cargo.lock ([[package]] blocks without a
#     `source`), so `--locked` builds work without running cargo.

set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "Usage: $0 <version>" >&2
  exit 1
fi

VERSION="$1"
RUST_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Writes the output of a filter over a file back to that file.
rewrite() {
  local file="$1"
  shift
  "$@" < "$file" > "$file.tmp"
  mv "$file.tmp" "$file"
}

find "$RUST_ROOT" -name Cargo.toml -not -path '*/target/*' -not -path '*/build/*' -print \
  | while IFS= read -r manifest; do
      rewrite "$manifest" sed -E \
        -e "s/^version = \"[^\"]*\"$/version = \"$VERSION\"/" \
        -e "s/(path = \"[^\"]*\", version = )\"[^\"]*\"/\1\"$VERSION\"/"
    done

rewrite "$RUST_ROOT/Cargo.lock" awk -v version="$VERSION" '
  BEGIN { RS = "" }
  /^\[\[package\]\]/ && !/\nsource = / {
    sub(/\nversion = "[^"]*"/, "\nversion = \"" version "\"")
  }
  { printf "%s%s", (NR > 1 ? "\n\n" : ""), $0 }
  END { print "" }
'
