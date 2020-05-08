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

# This script will update apache beam master branch with next release version
# and cut release branch for current development version.

# Parse parameters passing into the script

set -e

function usage() {
  echo 'Usage: set_version.sh <version> [--release] [--debug]'
}

IS_SNAPSHOT_VERSION=yes

while [[ $# -gt 0 ]] ; do
  arg="$1"

  case $arg in
      --release)
      unset IS_SNAPSHOT_VERSION
      shift
      ;;

      --debug)
      set -x
      shift
      ;;

      *)
      if [[ -z "$TARGET_VERSION" ]] ; then
        TARGET_VERSION="$1"
        shift
      else
        echo "Unknown argument: $1. Target version already set to $TARGET_VERSION"
        usage
        exit 1
      fi
      ;;
   esac
done

if [[ -z $TARGET_VERSION ]] ; then
  echo "No target version supplied"
  usage
  exit 1
fi

if [[ -z "$IS_SNAPSHOT_VERSION" ]] ; then
  # Fixing a release version
  sed -i -e "s/version=.*/version=$TARGET_VERSION/" gradle.properties
  sed -i -e "s/project.version = .*/project.version = '$TARGET_VERSION'/" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  sed -i -e "s/^__version__ = .*/__version__ = '${TARGET_VERSION}'/" sdks/python/apache_beam/version.py
  sed -i -e "s/sdk_version=.*/sdk_version=$TARGET_VERSION/" gradle.properties
  # TODO: [BEAM-4767]
  sed -i -e "s/'dataflow.container_version' : .*/'dataflow.container_version' : 'beam-${RELEASE}'/" runners/google-cloud-dataflow-java/build.gradle
else
  # For snapshot version:
  #   Java/gradle appends -SNAPSHOT
  #   In the Gradle plugin, the -SNAPSHOT is dynamic so we don't add it here
  #   Python appends .dev
  #   The Dataflow container remains unchanged as in this case it is beam-master-<date> form
  sed -i -e "s/version=.*/version=$TARGET_VERSION-SNAPSHOT/" gradle.properties
  sed -i -e "s/project.version = .*/project.version = '$TARGET_VERSION'/" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  sed -i -e "s/^__version__ = .*/__version__ = '${TARGET_VERSION}.dev'/" sdks/python/apache_beam/version.py
  sed -i -e "s/sdk_version=.*/sdk_version=$TARGET_VERSION.dev/" gradle.properties
  sed -i -e "s/'dataflow.container_version' : .*/'dataflow.container_version' : 'beam-master-.*'/" runners/google-cloud-dataflow-java/build.gradle
fi

