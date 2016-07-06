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

# This script will run pylint and pep8 on files that changed compared to the
# current HEAD of the branch.
#
# Use "pylint apache_beam" to run pylint all files.
# Use "pep8 apache_beam" to run pep8 all files.
#
# The exit-code of the script indicates success or a failure.

BASE_BRANCH=python-sdk

set -e
set -o pipefail

# Retrieve base branch for comparison. Travis does not fetch it by default.
git remote set-branches --add origin $BASE_BRANCH
git fetch

# Get the name of the files that changed compared to the HEAD of the branch.
# Use diff-filter to exclude deleted files. (i.e. Do not try to lint files that
# does not exist any more.) Filter the output to .py files only. Rewrite the
# paths relative to the sdks/python folder.
CHANGED_FILES=$(git diff --name-only --diff-filter=ACMRTUXB origin/$BASE_BRANCH . \
                | { grep ".py$" || true; }  \
                | sed 's/sdks\/python\///g')

if test "$CHANGED_FILES"; then
  echo "Running pylint on changed files:"
  echo "$CHANGED_FILES"
  pylint $CHANGED_FILES
  echo "Running pep8 on changed files:"
  pep8 $CHANGED_FILES
else
  echo "Not running pylint. No eligible files."
fi
