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

# This script uploads Python artifacts staged at dist.apache.org to PyPI.

set -e

function clean_up(){
  echo "Do you want to clean local clone repo ${LOCAL_CLONE_DIR}? [y|N]"
  read confirmation
  if [[ $confirmation = "y" ]]; then
    cd ~
    rm -rf ${LOCAL_CLONE_DIR}
    echo "Cleaned up local repo."
  fi
}

echo "Enter the release version, e.g. 2.21.0:"
read RELEASE
LOCAL_CLONE_DIR="beam_release_${RELEASE}"
cd ~
if [[ -d ${LOCAL_CLONE_DIR} ]]; then
  echo "Deleting existing local clone repo ${LOCAL_CLONE_DIR}."
  rm -rf ${LOCAL_CLONE_DIR}
fi
mkdir ${LOCAL_CLONE_DIR}
cd ${LOCAL_CLONE_DIR}

virtualenv deploy_pypi_env
source ./deploy_pypi_env/bin/activate
pip install twine

wget -r --no-parent -A zip,whl "https://dist.apache.org/repos/dist/dev/beam/${RELEASE}/python"
cd "dist.apache.org/repos/dist/dev/beam/${RELEASE}/python/"
echo "Will upload the following files to PyPI:"
ls
echo "Are the files listed correct? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Exiting without deploying artifacts to PyPI."
  clean_up
  exit
fi
twine upload *

clean_up
