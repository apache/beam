#!/bin/sh
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################


##############################################################################
##
##  Install groovy, if not installed. Source the script to get the path set.
##
##############################################################################

set -x

ZIP="https://dl.bintray.com/groovy/maven/apache-groovy-binary-2.4.13.zip"
DIR="${HOME}/.local/groovy"
rm -rf $DIR
mkdir -p $DIR
pushd $DIR
curl -L $ZIP --output groovy-binary.zip
unzip groovy-binary.zip
PATH=${DIR}/groovy-2.4.13/bin:$PATH
export PATH
popd
