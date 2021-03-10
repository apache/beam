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

# This script will deploy a Release Candidate to pypi, includes:
# 1. Build python artifacts with rc tag
# 2. Deploy Release Candidate to pypi

GIT_REPO_BASE_URL=apache/beam

echo "================Setting Up Environment Variables==========="
if [[ -z "${RELEASE}" ]]
then
  echo "Which release version are you working on: "
  read RELEASE
fi
RELEASE_BRANCH=release-${RELEASE}
if [[ -z "${RC_NUM}" ]]
then
  echo "Which release candidate number(e.g. 1) are you going to create: "
  read RC_NUM
fi
if [[ -z "${USER_GITHUB_ID}" ]]
then
  echo "Please enter your github username(ID): "
  read USER_GITHUB_ID
fi
echo ${RELEASE_BRANCH}
RELEASE_COMMIT=$(git rev-parse --verify ${RELEASE_BRANCH})

echo "================Checking Environment Variables=============="
echo "working on release version: ${RELEASE}"
echo "working on release branch: ${RELEASE_BRANCH}"
echo "will create release candidate: RC${RC_NUM}"
echo "Please review all environment variables and confirm: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

#
#echo "================Download python sources======================"
#echo "===============Build python artifacts with rc tag============"
#echo "====================Upload rc to pypi========================"