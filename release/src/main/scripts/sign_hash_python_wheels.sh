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

# This script will sign and hash python wheels.
set -e

BEAM_SVN_DIR=https://dist.apache.org/repos/dist/dev/beam
VERSION=
PYTHON_ARTIFACTS_DIR=python

echo "===============================Pre-requirements========================"
echo "Please make sure you have built python wheels."
echo "Please make sure you have configured and started your gpg by running ./preparation_before_release.sh."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please follow the release guide to build python wheels first."
  exit
fi

echo "[Input Required] Please enter the release version:"
read VERSION

echo "================Listing all GPG keys================="
gpg --list-keys --keyid-format LONG --fingerprint --fingerprint
echo "Please copy the public key which is associated with your Apache account:"

read SIGNING_KEY

cd ~
if [[ -d ${VERSION} ]]; then
  rm -rf ${VERSION}
fi

svn co ${BEAM_SVN_DIR}/${VERSION}
cd ${VERSION}/${PYTHON_ARTIFACTS_DIR}

echo "Fetch wheels artifacts"
gsutil cp -r gs://beam-wheels-staging/apache_beam-${VERSION}\*.whl .

echo "Start signing and hashing python wheels artifacts"
rm *.whl.asc || true
rm *.whl.sha512 ||true
for artifact in *.whl; do
  gpg --local-user ${SIGNING_KEY} --armor --detach-sig $artifact
  sha512sum $artifact > ${artifact}.sha512
done
svn add --force .
svn commit --no-auth-cache

rm -rf ~/${VERSION}
