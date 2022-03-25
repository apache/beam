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

# Get currently used Python version from Gradle or assume a default.
PYTHON=${1:-python3}
SCRIPT_DIR="${PWD}/license_scripts"
ENV_DIR="${PWD}/build/virtualenv"

# This file must already exist before this helper script is run.
# It is created by :sdks:java:container:generateLicenseReport
INDEX_FILE="${PWD}/build/reports/dependency-license/index.json"

# The licenses already pulled by generateDependencyReport are alongside index.json.
# The script first copies those over.
EXISTING_LICENSE_DIR="${PWD}/build/reports/dependency-license"

# The python will download Java licenses here
DOWNLOAD_DIR="${PWD}/build/target/java_third_party_licenses"

# All licenses will be put here by this script
DEST_DIR="${PWD}/build/target/third_party_licenses"

echo "Copying already-fetched licenses from ${EXISTING_LICENSE_DIR} to ${DOWNLOAD_DIR}"
if [ -d "$DOWNLOAD_DIR" ]; then rm -rf "$DOWNLOAD_DIR" ; fi
mkdir -p "$DOWNLOAD_DIR"
cp -r "${EXISTING_LICENSE_DIR}"/*.jar "${DOWNLOAD_DIR}"

$PYTHON -m venv --clear ${ENV_DIR} && . ${ENV_DIR}/bin/activate
pip install --retries 10 --upgrade pip setuptools wheel

# install packages
pip install --retries 10 -r ${SCRIPT_DIR}/requirement.txt

# pull licenses, notices and source code
FLAGS="--license_index=${INDEX_FILE} \
       --output_dir=${DOWNLOAD_DIR} \
       --dep_url_yaml=${SCRIPT_DIR}/dep_urls_java.yaml \
       --manual_license_path=${SCRIPT_DIR}/manual_licenses"

echo "Executing python ${SCRIPT_DIR}/pull_licenses_java.py $FLAGS"
python "${SCRIPT_DIR}/pull_licenses_java.py" $FLAGS

# If this script is running, it is assumed that outputs are out of date and should be cleared and rewritten
if [ -d "$DEST_DIR" ]; then rm -rf "$DEST_DIR"; fi
mkdir -p "$DEST_DIR"

echo "Copying licenses from ${DOWNLOAD_DIR} to ${DEST_DIR}."
cp -r "$DOWNLOAD_DIR"/*.jar "$DEST_DIR"/
cp -r "$DOWNLOAD_DIR"/*.csv "$DEST_DIR"/
gzip -r "$DEST_DIR"/*

echo "Finished license_scripts.sh"
