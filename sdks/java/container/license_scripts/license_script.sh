 # Licensed to the Apache Software Foundation (ASF) under one
 # or more contributor license agreements.  See the NOTICE file
 # distributed with this work for additional information
 # regarding copyright ownership.  The ASF licenses this file
 # to you under the Apache License, Version 2.0 (the
 # License); you may not use this file except in compliance
 # with the License.  You may obtain a copy of the License at
 #
 #     http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an AS IS BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.

set -e

ROOT=$(pwd)
SCRIPT_DIR="${ROOT}/sdks/java/container/license_scripts"
ENV_DIR="${ROOT}/sdks/java/container/build/virtualenv"
LICENSE_DIR="${ROOT}/sdks/java/container/third_party_licenses"

# reports are generated at ~/beam/java_third_party_licenses
./gradlew generateLicenseReport --rerun-tasks

# activate virtualenv
virtualenv ${ENV_DIR} && . ${ENV_DIR}/bin/activate

# install packages
${ENV_DIR}/bin/pip install -r ${SCRIPT_DIR}/requirement.txt

# pull licenses, notices and source code
FLAGS="--license_dir=${ROOT}/java_third_party_licenses \
       --dep_url_yaml=${SCRIPT_DIR}/dep_urls_java.yaml"
if [ "$1" = 'true' ]; then
  echo "Executing ${ENV_DIR}/bin/python ${SCRIPT_DIR}/pull_licenses_java.py $FLAGS --pull_licenses"
  ${ENV_DIR}/bin/python ${SCRIPT_DIR}/pull_licenses_java.py $FLAGS --pull_licenses
else
  echo "Executing ${ENV_DIR}/bin/python ${SCRIPT_DIR}/pull_licenses_java.py $FLAGS"
  ${ENV_DIR}/bin/python ${SCRIPT_DIR}/pull_licenses_java.py $FLAGS
fi

mkdir -p ${LICENSE_DIR}
if [ "$1" = 'true' ]; then
  echo "Copy licenses to sdks/java/container/third_party_licenses/."
  cp -r ${ROOT}/java_third_party_licenses/*.jar ${LICENSE_DIR}/
  cp -r ${ROOT}/java_third_party_licenses/*.csv ${LICENSE_DIR}/
  gzip -r ${LICENSE_DIR}/*
else
  # create an empty file to avoid no file/dir existing error
  echo "Create empty file."
  touch ${LICENSE_DIR}/empty
fi

rm -rf ${ROOT}/java_third_party_licenses
echo "Finished license_scripts.sh"