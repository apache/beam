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
  echo 'Usage: update_li_version.sh <version> [--snapshot] [--debug] [--git-add] [--python]'
}

GIT_ADD=no
PYTHON=no

while [[ $# -gt 0 ]] ; do
  arg="$1"

  case $arg in
      --snapshot)
      IS_SNAPSHOT_VERSION=yes
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

      --python)
      PYTHON=yes
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
  # obtain the current version
  LAST_VERSION=$(grep "project.version = '" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy | egrep -o "\d+\.\d+\.\d+\.\d+")
  # bump up one version
  TARGET_VERSION=$(awk -F. '{$NF++; print}' OFS=. <<< $LAST_VERSION)
  echo "Updating from $LAST_VERSION to $TARGET_VERSION"
  if [[ -z $TARGET_VERSION ]] ; then
    echo "Cannot obtain current version, please supply it in the command"
    usage
    exit 1
  fi
fi

if [[ -z "$IS_SNAPSHOT_VERSION" ]] ; then
  # Update the release version
  sed -i "" -e "s/version=.*/version=$TARGET_VERSION/" gradle.properties
  sed -i "" -e "s/sdk_version=.*/sdk_version=$TARGET_VERSION/" gradle.properties
  sed -i "" -e "s/project.version = .*/project.version = '$TARGET_VERSION'/" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  if [[ "$PYTHON" == yes ]] ; then
    sed -i "" -e "s/^__version__ = .*/__version__ = '${TARGET_VERSION}'/" sdks/python/apache_beam/version.py
  fi
else
  # For snapshot version:
  #   Java/gradle appends -SNAPSHOT
  #   In the Gradle plugin, the -SNAPSHOT is dynamic so we don't add it here
  #   Python appends .dev
  sed -i "" -e "s/version=.*/version=$TARGET_VERSION/" gradle.properties
  sed -i "" -e "s/project.version = .*/project.version = '$TARGET_VERSION-SNAPSHOT'/" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  sed -i "" -e "s/sdk_version=.*/sdk_version=$TARGET_VERSION.dev/" gradle.properties
  if [[ "$PYTHON" == yes ]] ; then
    sed -i "" -e "s/^__version__ = .*/__version__ = '${TARGET_VERSION}.dev'/" sdks/python/apache_beam/version.py
  fi
fi

if [[ "$GIT_ADD" == yes ]] ; then
  git add gradle.properties
  git add buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
  git add sdks/python/apache_beam/version.py
  git add sdks/go/pkg/beam/core/core.go
  git add runners/google-cloud-dataflow-java/build.gradle
fi
