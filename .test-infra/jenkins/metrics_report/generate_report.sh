#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script will be run by Jenkins as a Metric Report.
# Versus the tip of the target branch
# Parameters
#  $1: INFLUX_DB
#  $2: INFLUX_HOST
#  $3: INFLUX_PORT
#

set -e
set -v

VENV_DIR="metrics_report/venv"

INFLUX_DB=$1
INFLUX_HOST=$2
INFLUX_PORT=$3

echo "INFLUX_DB = ${INFLUX_DB}"
echo "INFLUX_HOST = ${INFLUX_HOST}"
echo "INFLUX_PORT = ${INFLUX_PORT}"

python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
cd "${WORKSPACE}/src/.test-infra/jenkins/metrics_report"
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install -r requirements.txt
python3 report_generator.py --influx-db="${INFLUX_DB}" --influx-host="${INFLUX_HOST}" --influx-port="${INFLUX_PORT}"
