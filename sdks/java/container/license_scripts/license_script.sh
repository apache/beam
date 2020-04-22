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
# reports are generated at ~/beam/java_third_party_licenses
./gradlew generateLicenseReport --rerun-tasks

# install packages needed for pull_licenses_java.py
pip install "beautifulsoup4>=4.9.0,<5.0"
pip install "future>=0.16.0,<1.0.0"
pip install "pyyaml>=3.12,<6.0.0"
pip install "tenacity>=5.0.2,<6.0"

# pull licenses, notices and source code
if [ "$1" = 'true' ]; then
  echo "Executing python sdks/java/container/license_scripts/pull_licenses_java.py --pull_licenses"
  python sdks/java/container/license_scripts/pull_licenses_java.py --pull_licenses
else
  echo "Executing python sdks/java/container/license_scripts/pull_licenses_java.py"
  python sdks/java/container/license_scripts/pull_licenses_java.py
fi

echo "Uninstall python packages used by pull_licenses_java.py."
pip uninstall -y beautifulsoup4
pip uninstall -y future
pip uninstall -y PyYAML
pip uninstall -y tenacity

mkdir sdks/java/container/third_party_licenses
if [ "$1" = 'true' ]; then
  echo "Copy licenses to sdks/java/container/third_party_licenses/."
  cp -r java_third_party_licenses/*.jar sdks/java/container/third_party_licenses/
  cp -r java_third_party_licenses/*.csv sdks/java/container/third_party_licenses/
else
  # create an empty file to avoid no file/dir existing error
  echo "Create empty file."
  touch sdks/java/container/third_party_licenses/empty
fi
rm -rf java_third_party_licenses
echo "Finished license_scripts.sh"
