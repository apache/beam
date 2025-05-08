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

# This script choose a commit to be the basis of a release candidate
# and pushed a new tagged commit for that RC.

set -e

function usage() {
  echo 'Usage: choose_rc_commit.sh --release <version> --rc <rc> --commit <commit> [--debug] [--clone] [--push-tag]'
}

RELEASE=
RC=
COMMIT=
PUSH_TAG=no
CLONE=no
OVERWRITE=no
DEBUG=
GIT_REPO=git@github.com:apache/beam

while [[ $# -gt 0 ]] ; do
  arg="$1"

  case $arg in
      --release)
      shift
      RELEASE=$1
      shift
      ;;

      --rc)
      shift
      RC=$1
      shift
      ;;

      --commit)
      shift
      COMMIT=$1
      shift
      ;;

      --debug)
      DEBUG=--debug
      set -x
      shift
      ;;

      --push-tag)
      PUSH_TAG=yes
      shift
      ;;

      --overwrite)
      OVERWRITE=yes
      shift
      ;;

      --clone)
      CLONE=yes
      shift
      ;;

      *)
      usage
      exit 1
      ;;
   esac
done

if [[ -z "$RELEASE" ]] ; then
  echo 'No release version supplied.'
  usage
  exit 1
fi

if [[ -z "$RC" ]] ; then
  echo 'No RC number supplied'
  usage
  exit 1
fi

if [[ -z "$COMMIT" ]] ; then
  echo 'No commit hash supplied.'
  usage
  exit 1
fi

SCRIPT_DIR=$(dirname $0)

RC_TAG="v${RELEASE}-RC${RC}"

RELEASE_BRANCH="release-$(cut -d '.' -f 1,2 <<< $RELEASE)"

if [[ "$CLONE" == yes ]] ; then
  CLONE_DIR=`mktemp -d`
  git clone "$GIT_REPO" "$CLONE_DIR" --single-branch --branch "$RELEASE_BRANCH" --shallow-exclude master
else
  echo "Not cloning repo; assuming working dir is the desired repo. To run with a fresh clone, run with --clone."
  CLONE_DIR=$PWD
fi

{
  cd "$CLONE_DIR"
  bash "$SCRIPT_DIR/set_version.sh" "${RELEASE}" --release --git-add $DEBUG
  git checkout --quiet "$COMMIT" # suppress warning about detached HEAD: we want it detached so we do not edit the branch
  git commit -m "Set version for ${RELEASE} RC${RC}"

  if git rev-parse "$RC_TAG" >/dev/null 2>&1; then
    if [[ "$OVERWRITE" == yes ]]; then
      git push origin ":refs/tags/$RC_TAG"
    else
      echo "Tag $RC_TAG already exists. Either delete it manually or run with --overwrite. Do not overwrite if an RC has been built and shared!"
      exit 1
    fi
  fi

  # Tag for Go SDK.
  # Go Modules defined in sub directories need to have a prefixed tag
  # in order to get the matching version.
  # See BEAM-13119 for context.
  git tag -a "sdks/$RC_TAG" -m "Go SDK $RC_TAG" HEAD

  # Primary tag for the repo.
  git tag -a -m "$RC_TAG" "$RC_TAG" HEAD

  if [[ "$PUSH_TAG" == yes ]] ; then
    git push --follow-tags origin "sdks/$RC_TAG"
    git push --follow-tags origin "$RC_TAG"
  else 
    echo "Not pushing tag $RC_TAG. You can push it manually or run with --push-tag."
  fi
}
