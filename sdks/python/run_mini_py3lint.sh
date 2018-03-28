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

# This script will run python3 ready style checks
#
#   Currently only flake8 E999
#
# The exit-code of the script indicates success or a failure.

set -o errexit
set -o pipefail

MODULE=apache_beam

usage(){ echo "Usage: $0 [MODULE|--help]  # The default MODULE is $MODULE"; }

if test $# -gt 0; then
  case "$@" in
    --help) usage; exit 1;;
	 *)      MODULE="$*";;
  esac
fi

echo "Running flake8 for module $MODULE:"
# TODO: Add F821 (undefined names) as soon as that test passes
flake8 $MODULE --count --select=E9,F822,F823 --show-source --statistics
