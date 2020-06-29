#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#####################################################################################
# Script to install virtual environment, set up virtual environment, and run Python command
# to execute Beam pipeline on Dataflow.

WORKSPACE=$1                # Path to the workspace with the source code
BUILD_DIRECTORY=$2          # Path to the build directory to activate the virtual environment in
MAIN_CLASS=$3               # Path to main class with the pipeline code
PIPELINE_OPTIONS=$4         # Pipeline options
BUILD_RELEASE_OPTIONS=$5    # Build release options

PYTHON_VERSION3="python3 -c 'import sys; print(sys.version_info.major == 3 and sys.version_info.minor>=5)'"
echo temp
if [[ "$PYTHON_VERSION3" == *"command not found"* || "$PYTHON_VERSION3" == "False" ]]; then
  echo "Must have Python 3.5 or greater to run pipeline on Dataflow."
  exit
fi

# Set directory to be inside current build folder
cd "$BUILD_DIRECTORY" || exit

# Install Python Virtual Environment
pip3 install --upgrade virtualenv

# Create a Virtual Environment "env"
virtualenv env

# Start up Virtual Environment
. env/bin/activate

# Install Apache Beam and Upgrade Six to be compatible with python3
pip3 install apache-beam
pip3 install apache-beam[gcp]
pip3 install six==1.12.0

# Set directory to be inside workspace to access Beam pipeline code
cd "$WORKSPACE" || exit

if [ "$BUILD_RELEASE_OPTIONS" != "" ]; then
  echo "Build Release Options included."
  python3 $MAIN_CLASS $PIPELINE_OPTIONS $BUILD_RELEASE_OPTIONS
else
  echo "No Build Release Options."
  python3 $MAIN_CLASS $PIPELINE_OPTIONS
fi


