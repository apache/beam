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

set -euo pipefail

# Create temporary directory.
TMPDIR=$(mktemp -d -t install_macos_deps.XXXXXX)
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "Using temporary directory: $TMPDIR"
cd "$TMPDIR"

# Download and extract FreeTDS.
curl -LO https://www.freetds.org/files/stable/freetds-1.5.2.tar.gz
tar -xzf freetds-1.5.2.tar.gz
cd freetds-1.5.2

# Configure, build and install FreeTDS.
./configure --prefix="$HOME/freetds" --with-tdsver=7.4
make
make install

# Set environment variables for pymssql installation.
export CFLAGS="-I$HOME/freetds/include"
export LDFLAGS="-L$HOME/freetds/lib"

echo "FreeTDS installed to $HOME/freetds"
