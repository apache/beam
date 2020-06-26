#!/bin/bash
# Script to install virtual environment, set up virtual environment, and run Python command to execute Beam pipeline on Dataflow

WORKSPACE=$1
BUILD_DIRECTORY=$2
MAIN_CLASS=$3
PIPELINE_OPTIONS=$4
BUILD_RELEASE_OPTIONS=$5

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


