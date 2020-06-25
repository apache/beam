#!/bin/bash
# Script to install virtual environment, set up virtual environment, and run Python command to execute Beam pipeline on Dataflow

#cd "${0%/*}" # Set directory to be directory it was executed in
cd $1
MAIN_CLASS=$2
PIPELINE_OPTIONS=$3
BUILD_RELEASE_OPTIONS=$4

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

if [ "$BUILD_RELEASE_OPTIONS" != "" ]; then
  echo "Build Release Options included."
  python3 $MAIN_CLASS $PIPELINE_OPTIONS $BUILD_RELEASE_OPTIONS
else
  echo "No Build Release Options."
  python3 $MAIN_CLASS $PIPELINE_OPTIONS
fi

# Deactivate Virtual Environment when job is finished
deactivate


