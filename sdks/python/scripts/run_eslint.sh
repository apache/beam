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

# The script runs eslint checks for all known typescript projects in the Beam
# repo.

set -e

# jlpm needs Node.js to work.
if ! command -v node; then
  echo "Node.js is not installed. Cannot execute the tests."
  exit 1
fi

echo "Running eslint checks..."

# Source needed to run tests are copied to this build dir by the copyTsSource
# task.
pushd ../../../ts

# Root dir for all Beam jupyterlab extensions.
LAB_EXT_DIR="sdks/python/apache_beam/runners/interactive/extensions"

known_test_dirs=( \
  "$LAB_EXT_DIR/apache-beam-jupyterlab-sidepanel" \
)

for dir in $known_test_dirs; do
  pushd $dir
  jlpm
  jlpm eslint:check
  popd
done

popd
