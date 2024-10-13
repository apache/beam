#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This is a script rather than inlined in package.json so as to be portable
# for windows.

set -e

# Using npx to execute tspc from the local node_modules environment.
npx tspc -p .

# Copy the python bootstrap script.
mkdir -p dist/resources
cp ../java/extensions/python/src/main/resources/org/apache/beam/sdk/extensions/python/bootstrap_beam_venv.py dist/resources
