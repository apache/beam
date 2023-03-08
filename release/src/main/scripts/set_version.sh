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

# This script will update the current checked out branch to be the
# specified version, either for release or development.
#
# This script should be the source of truth for all the locations in
# the codebase that require update.

set -e

function usage() {
  echo 'Usage: set_version.sh <version> [--release] [--debug] [--git-add]'
}

IS_SNAPSHOT_VERSION=yes
GIT_ADD=no

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

      --git-add)
      GIT_ADD=yes
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
  sed -i -e "s/SdkVersion = .*/SdkVersion = \"$TARGET_VERSION\"/" sdks/go/pkg/beam/core/core.go
  sed -i -e "s/\"version\": .*/\"version\": \"$TARGET_VERSION\",/" sdks/typescript/package.json
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
  sed -i -e "s/SdkVersion = .*/SdkVersion = \"${TARGET_VERSION}.dev\"/" sdks/go/pkg/beam/core/core.go
  sed -i -e "s/\"version\": .*/\"version\": \"$TARGET_VERSION-SNAPSHOT\",/" sdks/typescript/package.json
fi

if [[ "$GIT_ADD" == yes ]] ; then
  git add gradle.properties
  git add buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  git add sdks/python/apache_beam/version.py
  git add sdks/go/pkg/beam/core/core.go
  git add runners/google-cloud-dataflow-java/build.gradle
  git add sdks/typescript/package.json
fi
