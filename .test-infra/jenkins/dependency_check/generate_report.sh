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
# This script will be run by Jenkins as a Python dependency test.

set -e
set -v

# Get currently used Python version from args or assume a default.
PYTHON=${1:-python3}

REPORT_DESCRIPTION="
<h4> A dependency update is high priority if it satisfies one of following criteria: </h4>
<ul>
<li> It has major versions update available, e.g. org.assertj:assertj-core 2.5.0 -> 3.10.0; </li>
</ul>
<ul>
<li> It is over 3 minor versions behind the latest version, e.g. org.tukaani:xz 1.5 -> 1.8; </li>
</ul>
<ul>
<li> The current version is behind the later version for over 180 days, e.g. com.google.auto.service:auto-service 2014-10-24 -> 2017-12-11. </li>
</ul>
<h4> In Beam, we make a best-effort attempt at keeping all dependencies up-to-date.
     In the future, issues will be filed and tracked for these automatically,
     but in the meantime you can search for existing issues or open a new one.
</h4>
<h4> For more information: <a href=\"https://beam.apache.org/contribute/dependencies/\"> Beam Dependency Guide </a></h4>"


# Virtualenv for the rest of the script to run setup
$PYTHON -m venv dependency/check

. ./dependency/check/bin/activate
pip install --upgrade pip setuptools wheel
pip install --upgrade google-cloud-bigquery google-cloud-bigtable google-cloud-core
rm -f build/dependencyUpdates/beam-dependency-check-report.txt

# Install packages and run the unit tests of the report generator
pip install mock pyyaml
cd $WORKSPACE/src/.test-infra/jenkins
$PYTHON -m dependency_check.dependency_check_report_generator_test
$PYTHON -m dependency_check.version_comparer_test

echo "<html><body>" > $WORKSPACE/src/build/dependencyUpdates/beam-dependency-check-report.html

$PYTHON -m dependency_check.dependency_check_report_generator Python

$PYTHON -m dependency_check.dependency_check_report_generator Java

echo "$REPORT_DESCRIPTION </body></html>" >> $WORKSPACE/src/build/dependencyUpdates/beam-dependency-check-report.html
