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
##  Install Maven, to make sure at right version. Source the script to get the path set.
##
##############################################################################

set -x

ZIP="http://www.apache.org/dist/maven/maven-3/3.5.2/binaries/apache-maven-3.5.2-bin.tar.gz"
DIR="${HOME}/.local/maven"
rm -rf $DIR
mkdir -p $DIR
pushd $DIR
curl -L $ZIP --output maven.tar.gz
tar xf maven.tar.gz
PATH=$(pwd)/apache-maven-3.5.2/bin:$PATH
export PATH
popd
