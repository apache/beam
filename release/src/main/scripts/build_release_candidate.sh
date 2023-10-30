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

# This script will create a Release Candidate, includes:
# 1. Stage source release on dist.apache.org
# 2. Stage python source distribution and wheels on dist.apache.org
# 3. Stage SDK docker images
# 4. Create a PR to update beam-site

set -e

if [[ "$JAVA_HOME" ]]; then
  version=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ ! `echo $version | sed "s/1\.8\..*/1.8/"` == "1.8" ]]; then
    echo "Java version $version detected. Set \$JAVA_HOME to point to a JDK 8 installation."
    exit 1
  fi
else
  echo "\$JAVA_HOME must be set."
  exit 1
fi

SCRIPT_DIR="${PWD}/$(dirname $0)"
LOCAL_CLONE_DIR=build_release_candidate
LOCAL_JAVA_STAGING_DIR=java_staging_dir
LOCAL_PYTHON_STAGING_DIR=python_staging_dir
LOCAL_PYTHON_VIRTUALENV=${LOCAL_PYTHON_STAGING_DIR}/venv
LOCAL_WEBSITE_UPDATE_DIR=website_update_dir
LOCAL_PYTHON_DOC=python_doc
LOCAL_TYPESCRIPT_DOC=typescript_doc
LOCAL_JAVA_DOC=java_doc
LOCAL_WEBSITE_REPO=beam_website_repo

USER_REMOTE_URL=
USER_GITHUB_ID=
GIT_REPO_BASE_URL=apache/beam
GIT_REPO_URL=https://github.com/${GIT_REPO_BASE_URL}
ROOT_SVN_URL=https://dist.apache.org/repos/dist/dev/beam
GIT_BEAM_ARCHIVE=https://github.com/apache/beam/archive
GIT_BEAM_WEBSITE=https://github.com/apache/beam-site.git

PYTHON_ARTIFACTS_DIR=python
BEAM_ROOT_DIR=beam
WEBSITE_ROOT_DIR=beam-site

function usage() {
  echo 'Usage: build_release_candidate.sh --release <version> --rc <rc> --github-user <username> --signing-key <sig> [--debug]'
}

function wipe_local_clone_dir() {
  if [[ -d ${LOCAL_CLONE_DIR} ]]; then
    # Go modules leave behind directories with no write permissions, so readd
    # write perms to allow rm to work. Also, some files are owned by root and
    # will fail chmod, hence the `|| true`. Luckily they're world writable.
    chmod -R u+w ${LOCAL_CLONE_DIR} || true
    rm -rf ${LOCAL_CLONE_DIR}
  fi
}

RELEASE=
RC_NUM=
SIGNING_KEY=
USER_GITHUB_ID=
DEBUG=
JAVA11_HOME=

while [[ $# -gt 0 ]] ; do
  arg="$1"

  case $arg in
      --release)
      shift; RELEASE=$1; shift
      ;;

      --rc)
      shift; RC_NUM=$1; shift
      ;;

      --debug)
      DEBUG=--debug
      set -x; shift
      ;;

      --signing-key)
      shift; SIGNING_KEY=$1; shift
      ;;

      --github-user)
      shift; USER_GITHUB_ID=$1; shift
      ;;

      --java11-home)
      shift; JAVA11_HOME=$1; shift
      ;;

      *)
      echo "Unrecognized argument: $1"
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

if [[ -z "$RC_NUM" ]] ; then
  echo 'No RC number supplied.'
  usage
  exit 1
fi

if [[ -z "$RC_NUM" ]] ; then
  echo 'No RC number supplied.'
  usage
  exit 1
fi

if [[ -z "$USER_GITHUB_ID" ]] ; then
  echo 'Please provide your github username(ID)'
  usage
  exit 1
fi

if [[ -z "$JAVA11_HOME" ]] ; then
  echo 'Please provide Java 11 home. Required to build sdks/java/container/agent for Java 11+ containers.'
  usage
  exit 1
fi

if [[ -z "$SIGNING_KEY" ]] ; then
  echo "=================Pre-requirements===================="
  echo "Please make sure you have configured and started your gpg-agent by running ./preparation_before_release.sh."
  echo "================Listing all GPG keys================="
  echo "Please provide the public key to sign the release artifacts with. You can list them with this command:"
  echo ""
  echo "    gpg --list-keys --keyid-format LONG --fingerprint --fingerprint"
  echo ""
  usage
  exit 1
fi


RC_TAG="v${RELEASE}-RC${RC_NUM}"
USER_REMOTE_URL=git@github.com:${USER_GITHUB_ID}/beam-site

echo "================Checking Environment Variables=============="
echo "beam repo will be cloned into: ${LOCAL_CLONE_DIR}"
echo "working on release version: ${RELEASE}"
echo "will create release candidate: RC${RC_NUM} from commit tagged ${RC_TAG}"
echo "Your forked beam-site URL: ${USER_REMOTE_URL}"
echo "Your signing key: ${SIGNING_KEY}"
echo "Please review all environment variables and confirm: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "[Current Step]: Stage source release on dist.apache.org"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "=========Staging Source Release on dist.apache.org==========="
  cd ~
  if [[ -d ${LOCAL_JAVA_STAGING_DIR} ]]; then
    rm -rf ${LOCAL_JAVA_STAGING_DIR}
  fi
  mkdir -p ${LOCAL_JAVA_STAGING_DIR}
  cd ${LOCAL_JAVA_STAGING_DIR}
  svn co ${ROOT_SVN_URL}
  mkdir -p beam/${RELEASE}
  cd beam/${RELEASE}

  echo "----------------Downloading Source Release-------------------"
  # GitHub strips the "v" from "v2.29.0" in naming zip and the dir inside it
  RC_DIR="beam-${RELEASE}-RC${RC_NUM}"
  RC_ZIP="${RC_DIR}.tar.gz"
  # We want to strip the -RC1 suffix from the directory name inside the zip
  RELEASE_DIR="beam-${RELEASE}"

  SOURCE_RELEASE_ZIP="apache-beam-${RELEASE}-source-release.tar.gz"
  # Check whether there is an existing dist dir
  if (svn ls "${SOURCE_RELEASE_ZIP}"); then
    echo "Removing existing ${SOURCE_RELEASE_ZIP}."
    svn delete "${SOURCE_RELEASE_ZIP}"
  fi

  echo "Downloading: ${GIT_BEAM_ARCHIVE}/${RC_TAG}.tar.gz"
  wget ${GIT_BEAM_ARCHIVE}/${RC_TAG}.tar.gz  -O "${RC_ZIP}"

  unzip "$RC_ZIP"
  rm "$RC_ZIP"
  mv "$RC_DIR" "$RELEASE_DIR"
  zip -r "${SOURCE_RELEASE_ZIP}" "$RELEASE_DIR"
  rm -r "$RELEASE_DIR"

  echo "----Signing Source Release ${SOURCE_RELEASE_ZIP}-----"
  gpg --local-user ${SIGNING_KEY} --armor --batch --yes --detach-sig "${SOURCE_RELEASE_ZIP}"

  echo "----Creating Hash Value for ${SOURCE_RELEASE_ZIP}----"
  sha512sum ${SOURCE_RELEASE_ZIP} > ${SOURCE_RELEASE_ZIP}.sha512

  svn add --force .
  svn status
  echo "Please confirm these changes are ready to commit: [y|N] "
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Exit without staging source release on dist.apache.org."
  else
    svn commit --no-auth-cache -m "Staging Java artifacts for Apache Beam ${RELEASE} RC${RC_NUM}"
  fi
  rm -rf ~/${LOCAL_JAVA_STAGING_DIR}
fi


echo "[Current Step]: Stage python source distribution and wheels on dist.apache.org"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Staging Python Binaries on dist.apache.org========="
  cd ~
  if [[ -d "${LOCAL_PYTHON_STAGING_DIR}" ]]; then
    rm -rf "${LOCAL_PYTHON_STAGING_DIR}"
  fi
  mkdir -p "${LOCAL_PYTHON_STAGING_DIR}"
  cd "${LOCAL_PYTHON_STAGING_DIR}"

  echo '-------------------Cloning Beam Release Branch-----------------'
  git clone --branch "${RC_TAG}" --depth 1 "${GIT_REPO_URL}"
  cd "${BEAM_ROOT_DIR}"
  RELEASE_COMMIT=$(git rev-list -n 1 "tags/${RC_TAG}")

  echo '-------------------Creating Python Virtualenv-----------------'
  python3 -m venv "${LOCAL_PYTHON_VIRTUALENV}"
  source "${LOCAL_PYTHON_VIRTUALENV}/bin/activate"
  pip install --upgrade pip setuptools wheel
  pip install requests python-dateutil

  echo '--------------Fetching GitHub Actions Artifacts--------------'
  SVN_ARTIFACTS_DIR="beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}"
  svn co https://dist.apache.org/repos/dist/dev/beam
  mkdir -p "${SVN_ARTIFACTS_DIR}"
  python "${SCRIPT_DIR}/download_github_actions_artifacts.py" \
    --github-user "${USER_GITHUB_ID}" \
    --repo-url "${GIT_REPO_BASE_URL}" \
    --rc-tag "${RC_TAG}" \
    --release-commit "${RELEASE_COMMIT}" \
    --artifacts_dir "${SVN_ARTIFACTS_DIR}"

  cd "${SVN_ARTIFACTS_DIR}"

  echo "------Checking Hash Value for apache-beam-${RELEASE}.tar.gz-----"
  sha512sum -c "apache-beam-${RELEASE}.tar.gz.sha512"

  echo "------Signing Source Release apache-beam-${RELEASE}.tar.gz------"
  gpg --local-user "${SIGNING_KEY}" --armor --detach-sig "apache-beam-${RELEASE}.tar.gz"

  for artifact in *.whl; do
    echo "----------Checking Hash Value for ${artifact} wheel-----------"
    sha512sum -c "${artifact}.sha512"
  done

  for artifact in *.whl; do
    echo "------------------Signing ${artifact} wheel-------------------"
    gpg --local-user "${SIGNING_KEY}" --armor --batch --yes --detach-sig "${artifact}"
  done

  cd ..
  svn add --force ${PYTHON_ARTIFACTS_DIR}
  svn status
  echo "Please confirm these changes are ready to commit: [y|N] "
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Exit without staging python artifacts on dist.apache.org."
  else
    svn commit --no-auth-cache -m "Staging Python artifacts for Apache Beam ${RELEASE} RC${RC_NUM}"
  fi
  rm -rf "${HOME:?}/${LOCAL_PYTHON_STAGING_DIR}"
fi

echo "[Current Step]: Stage docker images"
echo "Note: this step will also prune your local docker image and container cache."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  docker system prune -a -f
  echo "============Staging SDK docker images on docker hub========="
  cd ~
  wipe_local_clone_dir
  mkdir -p ${LOCAL_CLONE_DIR}
  cd ${LOCAL_CLONE_DIR}

  echo '-------------------Cloning Beam RC Tag-----------------'
  git clone --depth 1 --branch "${RC_TAG}" ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RC_TAG}

  ./gradlew :pushAllDockerImages -PisRelease -Pdocker-pull-licenses -Pdocker-tag=${RELEASE}rc${RC_NUM} -Pjava11Home=${JAVA11_HOME} --no-daemon --no-parallel

  wipe_local_clone_dir
fi

echo "[Current Step]: Update beam-site"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "==============Creating PR for Updating Website==============="
  cd ~
  if [[ -d ${LOCAL_WEBSITE_UPDATE_DIR} ]]; then
    rm -rf ${LOCAL_WEBSITE_UPDATE_DIR}
  fi
  mkdir -p ${LOCAL_WEBSITE_UPDATE_DIR}
  cd ${LOCAL_WEBSITE_UPDATE_DIR}
  mkdir -p ${LOCAL_PYTHON_DOC}
  mkdir -p ${LOCAL_TYPESCRIPT_DOC}
  mkdir -p ${LOCAL_JAVA_DOC}
  mkdir -p ${LOCAL_WEBSITE_REPO}

  echo "------------------Building Python Doc------------------------"
  python3 -m venv "${LOCAL_PYTHON_VIRTUALENV}"
  source "${LOCAL_PYTHON_VIRTUALENV}/bin/activate"
  pip install --upgrade pip setuptools wheel
  cd ${LOCAL_PYTHON_DOC}
  pip install -U pip
  pip install tox
  git clone --branch "${RC_TAG}" --depth 1 ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  RELEASE_COMMIT=$(git rev-list -n 1 "tags/${RC_TAG}")
  # TODO(https://github.com/apache/beam/issues/20209): Don't hardcode py version in this file.
  cd sdks/python && tox -e py38-docs
  GENERATED_PYDOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}/${BEAM_ROOT_DIR}/sdks/python/target/docs/_build
  rm -rf ${GENERATED_PYDOC}/.doctrees

  echo "------------------Building Typescript Doc------------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_TYPESCRIPT_DOC}
  git clone --branch "${RC_TAG}" --depth 1 ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  cd sdks/typescript && npm ci && npm run docs
  GENERATED_TYPEDOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_TYPESCRIPT_DOC}/${BEAM_ROOT_DIR}/sdks/typescript/docs

  echo "----------------------Building Java Doc----------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  git clone --branch "${RC_TAG}" --depth 1 ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  ./gradlew :sdks:java:javadoc:aggregateJavadoc -PisRelease --no-daemon --no-parallel
  GENERATE_JAVADOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}/${BEAM_ROOT_DIR}/sdks/java/javadoc/build/docs/javadoc/

  echo "------------------Updating Release Docs---------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_WEBSITE_REPO}
  git clone ${GIT_BEAM_WEBSITE}
  cd ${WEBSITE_ROOT_DIR}
  git checkout release-docs
  git checkout -b updates_release_${RELEASE} release-docs

  echo "..........Copying generated javadoc into beam-site.........."
  cp -r ${GENERATE_JAVADOC} javadoc/${RELEASE}
  # Update current symlink to point to the latest release
  unlink javadoc/current
  ln -s ${RELEASE} javadoc/current

  echo "............Copying generated pydoc into beam-site.........."
  cp -r ${GENERATED_PYDOC} pydoc/${RELEASE}
  # Update current symlink to point to the latest release
  unlink pydoc/current
  ln -s ${RELEASE} pydoc/current

  echo "............Copying generated typedoc into beam-site.........."
  mkdir -p typedoc
  cp -r ${GENERATED_TYPEDOC} typedoc/${RELEASE}
  # Update current symlink to point to the latest release
  unlink typedoc/current | true
  ln -s ${RELEASE} typedoc/current

  git add -A
  git commit -m "Update beam-site for release ${RELEASE}." -m "Content generated from commit ${RELEASE_COMMIT}."
  git push -f ${USER_REMOTE_URL}

  # Check if hub is installed. See https://stackoverflow.com/a/677212
  if ! hash hub 2> /dev/null; then
    echo "You don't have hub installed, do you want to install hub with sudo permission? [y|N]"
    read confirmation
    if [[ $confirmation = "y" ]]; then
      HUB_VERSION=2.5.0
      HUB_ARTIFACTS_NAME=hub-linux-amd64-${HUB_VERSION}
      wget https://github.com/github/hub/releases/download/v${HUB_VERSION}/${HUB_ARTIFACTS_NAME}.tgz
      tar zvxvf ${HUB_ARTIFACTS_NAME}.tgz
      sudo ./${HUB_ARTIFACTS_NAME}/install
      echo "eval "$(hub alias -s)"" >> ~/.bashrc
      rm -rf ${HUB_ARTIFACTS_NAME}*
    fi
  fi
  if hash hub 2> /dev/null; then
    hub pull-request -m "Publish ${RELEASE} release" -h ${USER_GITHUB_ID}:updates_release_${RELEASE} -b apache:release-docs \
     || echo "Pull request creation did not succeed. If you created the website PR earlier it may have been updated via force-push."
  else
    echo "Without hub, you need to create PR manually."
  fi

  echo "Finished v${RELEASE}-RC${RC_NUM} creation."
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_TYPESCRIPT_DOC}
fi
