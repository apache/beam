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
# 1. Build and stage java artifacts
# 2. Stage source release on dist.apache.org
# 3. Stage python binaries
# 4. Stage SDK docker images
# 5. Create a PR to update beam-site

set -e

LOCAL_CLONE_DIR=build_release_candidate
LOCAL_JAVA_STAGING_DIR=java_staging_dir
LOCAL_PYTHON_STAGING_DIR=python_staging_dir
LOCAL_PYTHON_VIRTUALENV=${LOCAL_PYTHON_STAGING_DIR}/venv
LOCAL_WEBSITE_UPDATE_DIR=website_update_dir
LOCAL_PYTHON_DOC=python_doc
LOCAL_JAVA_DOC=java_doc
LOCAL_WEBSITE_REPO=beam_website_repo

USER_REMOTE_URL=
USER_GITHUB_ID=
GIT_REPO_BASE_URL=apache/beam
GIT_REPO_URL=git@github.com:${GIT_REPO_BASE_URL}.git
ROOT_SVN_URL=https://dist.apache.org/repos/dist/dev/beam
GIT_BEAM_ARCHIVE=https://github.com/apache/beam/archive
GIT_BEAM_WEBSITE=https://github.com/apache/beam-site.git

PYTHON_ARTIFACTS_DIR=python
BEAM_ROOT_DIR=beam
WEBSITE_ROOT_DIR=beam-site

DOCKER_IMAGE_DEFAULT_REPO_ROOT=apache
DOCKER_IMAGE_DEFAULT_REPO_PREFIX=beam_

PYTHON_VER=("python2.7" "python3.5" "python3.6" "python3.7")
FLINK_VER=("1.8" "1.9" "1.10")

echo "================Setting Up Environment Variables==========="
echo "Which release version are you working on: "
read RELEASE
RELEASE_BRANCH=release-${RELEASE}
echo "Which release candidate number(e.g. 1) are you going to create: "
read RC_NUM
echo "Please enter your github username(ID): "
read USER_GITHUB_ID

USER_REMOTE_URL=git@github.com:${USER_GITHUB_ID}/beam-site

echo "================Listing all GPG keys================="
gpg --list-keys --keyid-format LONG --fingerprint --fingerprint
echo "Please copy the public key which is associated with your Apache account:"

read SIGNING_KEY

echo "================Checking Environment Variables=============="
echo "beam repo will be cloned into: ${LOCAL_CLONE_DIR}"
echo "working on release version: ${RELEASE}"
echo "working on release branch: ${RELEASE_BRANCH}"
echo "will create release candidate: RC${RC_NUM}"
echo "Your forked beam-site URL: ${USER_REMOTE_URL}"
echo "Your signing key: ${SIGNING_KEY}"
echo "Please review all environment variables and confirm: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

echo "[Current Step]: Build and stage java artifacts"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Building and Staging Java Artifacts============="
  echo "--------Cloning Beam Repo and Checkout Release Branch-------"
  cd ~
  if [[ -d ${LOCAL_CLONE_DIR} ]]; then
    rm -rf ${LOCAL_CLONE_DIR}
  fi
  mkdir -p ${LOCAL_CLONE_DIR}
  cd ${LOCAL_CLONE_DIR}
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  RELEASE_COMMIT=$(git rev-parse --verify ${RELEASE_BRANCH})

  echo "-------------Building Java Artifacts with Gradle-------------"
  git config credential.helper store

  ./gradlew release -Prelease.newVersion=${RELEASE}-SNAPSHOT \
                -Prelease.releaseVersion=${RELEASE}-RC${RC_NUM} \
                -Prelease.useAutomaticVersion=true --info --no-daemon

  git push origin "${RELEASE_BRANCH}"
  git push origin "v${RELEASE}-RC${RC_NUM}"

  echo "-------------Staging Java Artifacts into Maven---------------"
  gpg --local-user ${SIGNING_KEY} --output /dev/null --sign ~/.bashrc
  ./gradlew publish -Psigning.gnupg.keyName=${SIGNING_KEY} -PisRelease --no-daemon
  echo "You need to close the staging repository manually on Apache Nexus. See the release guide for instructions."
  rm -rf ~/${LOCAL_CLONE_DIR}
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
  SOURCE_RELEASE_ZIP="apache-beam-${RELEASE}-source-release.zip"
  # Check whether there is an existing dist dir
  if (svn ls "${SOURCE_RELEASE_ZIP}"); then
    echo "Removing existing ${SOURCE_RELEASE_ZIP}."
    svn delete "${SOURCE_RELEASE_ZIP}"
  fi

  echo "Downloading: ${GIT_BEAM_ARCHIVE}/release-${RELEASE}.zip"
  wget ${GIT_BEAM_ARCHIVE}/release-${RELEASE}.zip  -O "${SOURCE_RELEASE_ZIP}"

  echo "----Signing Source Release ${SOURCE_RELEASE_ZIP}-----"
  gpg --local-user ${SIGNING_KEY} --armor --detach-sig "${SOURCE_RELEASE_ZIP}"

  echo "----Creating Hash Value for ${SOURCE_RELEASE_ZIP}----"
  sha512sum ${SOURCE_RELEASE_ZIP} > ${SOURCE_RELEASE_ZIP}.sha512

  # The svn commit is interactive already and can be aborted by deleted the commit msg
  svn add --force .
  svn commit --no-auth-cache
  rm -rf ~/${LOCAL_JAVA_STAGING_DIR}
fi


echo "[Current Step]: Stage python binaries and wheels"
echo "===============================Pre-requirements========================"
echo "Please make sure you have configured and started your gpg by running ./preparation_before_release.sh."
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Staging Python Binaries on dist.apache.org========="
  cd ~
  if [[ -d ${LOCAL_PYTHON_STAGING_DIR} ]]; then
    rm -rf ${LOCAL_PYTHON_STAGING_DIR}
  fi
  mkdir -p ${LOCAL_PYTHON_STAGING_DIR}
  cd ${LOCAL_PYTHON_STAGING_DIR}

  echo '-------------------Cloning Beam Release Branch-----------------'
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  git push origin "${RELEASE_BRANCH}"
  RELEASE_COMMIT=$(git rev-parse --verify HEAD)

  echo '-------------------Creating Python Virtualenv-----------------'
  python3 -m venv ${LOCAL_PYTHON_VIRTUALENV}
  source ${LOCAL_PYTHON_VIRTUALENV}/bin/activate
  pip install requests python-dateutil

  echo '--------------Fetching GitHub Actions Artifacts--------------'
  python release/src/main/scripts/download_github_actions_artifacts.py \
    --github-token ${GITHUB_TOKEN} \
    --github-user ${USER_GITHUB_ID} \
    --repo-url ${GIT_REPO_BASE_URL} \
    --release-branch ${RELEASE_BRANCH} \
    --release-commit ${RELEASE_COMMIT} \
    --artifacts_dir ${PYTHON_ARTIFACTS_DIR}

  svn co https://dist.apache.org/repos/dist/dev/beam
  mkdir -p beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}
  cp -ar ${PYTHON_ARTIFACTS_DIR}/ beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}/
  cd beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}

  echo "------Signing Source Release apache-beam-${RELEASE}.zip------"
  gpg --local-user ${SIGNING_KEY} --armor --detach-sig apache-beam-${RELEASE}.zip

  echo "------Creating Hash Value for apache-beam-${RELEASE}.zip-----"
  sha512sum apache-beam-${RELEASE}.zip > apache-beam-${RELEASE}.zip.sha512

  echo "-----Signing Source Release apache-beam-${RELEASE}.tar.gz-----"
  gpg --local-user ${SIGNING_KEY} --armor --detach-sig apache-beam-${RELEASE}.zip

  echo "-----Creating Hash Value for apache-beam-${RELEASE}.tar.gz----"
  sha512sum apache-beam-${RELEASE}.zip > apache-beam-${RELEASE}.zip.sha512

  echo "---------Signing Release wheels apache-beam-${RELEASE}--------"
  for artifact in *.whl; do
    gpg --local-user ${SIGNING_KEY} --armor --detach-sig $artifact
  done

  echo "-----Creating Hash Value for apache-beam-${RELEASE} wheels-----"
  for artifact in *.whl; do
    sha512sum $artifact > ${artifact}.sha512
  done

  cd ..
  svn add --force ${PYTHON_ARTIFACTS_DIR}
  svn status
  echo "Please confirm these changes are ready to commit: [y|N] "
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Exit without staging python artifacts on dist.apache.org."
    rm -rf ~/${PYTHON_ARTIFACTS_DIR}
    exit
  fi
  svn commit --no-auth-cache
  rm -rf ~/${PYTHON_ARTIFACTS_DIR}
fi

echo "[Current Step]: Stage docker images"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Staging SDK docker images on docker hub========="
  cd ~
  if [[ -d ${LOCAL_PYTHON_STAGING_DIR} ]]; then
    rm -rf ${LOCAL_PYTHON_STAGING_DIR}
  fi
  mkdir -p ${LOCAL_PYTHON_STAGING_DIR}
  cd ${LOCAL_PYTHON_STAGING_DIR}

  echo '-------------------Cloning Beam Release Branch-----------------'
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}

  echo '-------------------Generating and Pushing Python images-----------------'
  ./gradlew :sdks:python:container:buildAll -Pdocker-pull-licenses -Pdocker-tag=${RELEASE}_rc${RC_NUM}
  for ver in "${PYTHON_VER[@]}"; do
    docker push ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_rc${RC_NUM} &
  done

  echo '-------------------Generating and Pushing Java images-----------------'
  ./gradlew :sdks:java:container:dockerPush -Pdocker-pull-licenses -Pdocker-tag=${RELEASE}_rc${RC_NUM}

  echo '-------------Generating and Pushing Flink job server images-------------'
  echo "Building containers for the following Flink versions:" "${FLINK_VER[@]}"
  for ver in "${FLINK_VER[@]}"; do
    ./gradlew ":runners:flink:${ver}:job-server-container:dockerPush" -Pdocker-tag="${RELEASE}_rc${RC_NUM}"
  done

  echo '-------------Generating and Pushing Spark job server image-------------'
  ./gradlew ":runners:spark:job-server:container:dockerPush" -Pdocker-tag="${RELEASE}_rc${RC_NUM}"

  rm -rf ~/${PYTHON_ARTIFACTS_DIR}

  echo '-------------------Clean up images at local-----------------'
  for ver in "${PYTHON_VER[@]}"; do
     docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}${ver}_sdk:${RELEASE}_rc${RC_NUM}
  done
  docker rmi -f ${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}java_sdk:${RELEASE}_rc${RC_NUM}
  for ver in "${FLINK_VER[@]}"; do
    docker rmi -f "${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}flink${ver}_job_server:${RELEASE}_rc${RC_NUM}"
  done
  docker rmi -f "${DOCKER_IMAGE_DEFAULT_REPO_ROOT}/${DOCKER_IMAGE_DEFAULT_REPO_PREFIX}spark_job_server:${RELEASE}_rc${RC_NUM}"
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
  mkdir -p ${LOCAL_JAVA_DOC}
  mkdir -p ${LOCAL_WEBSITE_REPO}

  echo "------------------Building Python Doc------------------------"
  virtualenv ${LOCAL_PYTHON_VIRTUALENV}
  source ${LOCAL_PYTHON_VIRTUALENV}/bin/activate
  cd ${LOCAL_PYTHON_DOC}
  pip install tox
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  cd sdks/python && pip install -r build-requirements.txt && tox -e py37-docs
  GENERATED_PYDOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}/${BEAM_ROOT_DIR}/sdks/python/target/docs/_build
  rm -rf ${GENERATED_PYDOC}/.doctrees

  echo "----------------------Building Java Doc----------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  ./gradlew :sdks:java:javadoc:aggregateJavadoc
  GENERATE_JAVADOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}/${BEAM_ROOT_DIR}/sdks/java/javadoc/build/docs/javadoc/

  echo "------------------Updating Release Docs---------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_WEBSITE_REPO}
  git clone ${GIT_BEAM_WEBSITE}
  cd ${WEBSITE_ROOT_DIR}
  git checkout release-docs
  git checkout -b updates_release_${RELEASE} release-docs

  echo "..........Copying generated javadoc into beam-site.........."
  cp -r ${GENERATE_JAVADOC} javadoc/${RELEASE}

  echo "............Copying generated pydoc into beam-site.........."
  cp -r ${GENERATED_PYDOC} pydoc/${RELEASE}

  git add -A
  git commit -m "Update beam-site for release ${RELEASE}\n\nContent generated based on commit ${RELEASE_COMMIT}"
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
    hub pull-request -m "Publish ${RELEASE} release" -h ${USER_GITHUB_ID}:updates_release_${RELEASE} -b apache:release-docs
  else
    echo "Without hub, you need to create PR manually."
  fi

  echo "Finished v${RELEASE}-RC${RC_NUM} creation."
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}
fi
