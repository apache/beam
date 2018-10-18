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
# 4. Create a PR to update beam-site

set -e

LOCAL_CLONE_DIR=build_release_candidate
LOCAL_JAVA_STAGING_DIR=java_staging_dir
LOCAL_PYTHON_STAGING_DIR=python_staging_dir
LOCAL_WEBSITE_UPDATE_DIR=website_update_dir
LOCAL_PYTHON_DOC=python_doc
LOCAL_JAVA_DOC=java_doc
LOCAL_WEBSITE_REPO=beam_website_repo

USER_REMOTE_URL=
USER_GITHUB_ID=
GIT_REPO_URL=https://github.com/apache/beam.git
ROOT_SVN_URL=https://dist.apache.org/repos/dist/dev/beam
GIT_BEAM_ARCHIVE=https://github.com/apache/beam/archive
GIT_BEAM_WEBSITE=https://github.com/apache/beam-site.git

PYTHON_ARTIFACTS_DIR=python
BEAM_ROOT_DIR=beam
WEBSITE_ROOT_DIR=beam-site


echo "================Setting Up Environment Variables==========="
echo "Which release version are you working on: "
read RELEASE
RELEASE_BRANCH=release-${RELEASE}
echo "Which release candidate number(e.g. 1) are you going to create: "
read RC_NUM
echo "Please enter your repo URL forked from apache/beam-site"
read USER_REMOTE_URL
echo "Please enter your github username(ID): "
read USER_GITHUB_ID

echo "================Checking Environment Variables=============="
echo "beam repo will be cloned into: ${LOCAL_CLONE_DIR}"
echo "working on release version: ${RELEASE}"
echo "working on release branch: ${RELEASE_BRANCH}"
echo "will create release candidate: RC${RC_NUM}"
echo "Your forked beam-site URL: ${USER_REMOTE_URL}"
echo "Please review all environment variables and confirm: [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Please rerun this script and make sure you have the right inputs."
  exit
fi

# Check whether we have already created RC1. If so, we need to delete ${RELEASE} dir first
if [[ ${RC_NUM} != "1" ]]; then
  echo "Needs to delete ${ROOT_SVN_URL}/${RELEASE} first."
  svn delete ${ROOT_SVN_URL}/${RELEASE}
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
  mkdir ${LOCAL_CLONE_DIR}
  cd ${LOCAL_CLONE_DIR}
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}

  echo "-------------Building Java Artifacts with Gradle-------------"
  git config credential.helper store

  ./gradlew release -Prelease.newVersion=${RELEASE}-SNAPSHOT \
                -Prelease.releaseVersion=${RELEASE}-RC${RC_NUM} \
                -Prelease.useAutomaticVersion=true --info --no-daemon

  echo "Please make sure gradle release succeed: "
  echo "1. release code has been pushed to github repo."
  echo "2. new rc tag has created in github."

  echo "-------------Staging Java Artifacts into Maven---------------"
  ./gradlew publish -PisRelease --no-parallel --no-daemon
  echo "Please review all artifacts in staging URL. e.g. https://repository.apache.org/content/repositories/orgapachebeam-1049/"
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
  mkdir ${LOCAL_JAVA_STAGING_DIR}
  cd ${LOCAL_JAVA_STAGING_DIR}
  svn co ${ROOT_SVN_URL}
  mkdir beam/${RELEASE}
  cd beam/${RELEASE}

  echo "----------------Downloading Source Release-------------------"
  echo "Downloading: ${GIT_BEAM_ARCHIVE}/release-${RELEASE}.zip"
  wget ${GIT_BEAM_ARCHIVE}/release-${RELEASE}.zip  -O apache-beam-${RELEASE}-source-release.zip

  echo "----Signing Source Release apache-beam-${RELEASE}-source-release.zip-----"
  gpg --armor --detach-sig apache-beam-${RELEASE}-source-release.zip

  echo "----Creating Hash Value for  apache-beam-${RELEASE}-source-release.zip----"
  sha512sum apache-beam-${RELEASE}-source-release.zip > apache-beam-${RELEASE}-source-release.zip.sha512

  svn add .
  svn status
  echo "Please confirm these changes are ready to commit: [y|N] "
  read confirmation
  if [[ $confirmation != "y" ]]; then
    echo "Exit without staging source release on dist.apache.org."
    rm -rf ~/${LOCAL_JAVA_STAGING_DIR}
    exit
  fi
  svn commit --no-auth-cache
  rm -rf ~/${LOCAL_JAVA_STAGING_DIR}
fi

echo "[Current Step]: Stage python binaries"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "============Staging Python Binaries on dist.apache.org========="
  cd ~
  if [[ -d ${LOCAL_PYTHON_STAGING_DIR} ]]; then
    rm -rf ${LOCAL_PYTHON_STAGING_DIR}
  fi
  mkdir ${LOCAL_PYTHON_STAGING_DIR}
  cd ${LOCAL_PYTHON_STAGING_DIR}

  echo '-------------------Cloning Beam Release Branch-----------------'
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}

  echo '-------------------Generating Python Artifacts-----------------'
  cd sdks/python
  python setup.py sdist --format=zip
  cd dist

  svn co https://dist.apache.org/repos/dist/dev/beam
  mkdir beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}
  cp apache-beam-${RELEASE}.zip beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}/apache-beam-${RELEASE}.zip
  cd beam/${RELEASE}/${PYTHON_ARTIFACTS_DIR}

  echo "------Signing Source Release apache-beam-${RELEASE}.zip------"
  gpg --armor --detach-sig apache-beam-${RELEASE}.zip

  echo "------Creating Hash Value for apache-beam-${RELEASE}.zip------"
  sha512sum apache-beam-${RELEASE}.zip > apache-beam-${RELEASE}.zip.sha512

  cd ..
  svn add ${PYTHON_ARTIFACTS_DIR}
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

echo "[Current Step]: Update beam-site"
echo "Do you want to proceed? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "==============Creating PR for Updating Website==============="
  cd ~
  if [[ -d ${LOCAL_WEBSITE_UPDATE_DIR} ]]; then
    rm -rf ${LOCAL_WEBSITE_UPDATE_DIR}
  fi
  mkdir ${LOCAL_WEBSITE_UPDATE_DIR}
  cd ${LOCAL_WEBSITE_UPDATE_DIR}
  mkdir ${LOCAL_PYTHON_DOC}
  mkdir ${LOCAL_JAVA_DOC}
  mkdir ${LOCAL_WEBSITE_REPO}

  echo "------------------Building Python Doc------------------------"
  cd ${LOCAL_PYTHON_DOC}
  pip install tox
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  cd sdks/python && tox -e docs
  GENERATED_PYDOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}/${BEAM_ROOT_DIR}/sdks/python/target/docs/_build
  rm -rf ${GENERATED_PYDOC}/.doctrees

  echo "----------------------Building Java Doc----------------------"
  cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  git clone ${GIT_REPO_URL}
  cd ${BEAM_ROOT_DIR}
  git checkout ${RELEASE_BRANCH}
  ./gradlew :beam-sdks-java-javadoc:aggregateJavadoc
  GENERATE_JAVADOC=~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}/${BEAM_ROOT_DIR}/sdks/java/javadoc/build/docs/javadoc/

  echo "------------------Updating Beam Website---------------------"
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
  git commit -m "Update beam-site for release ${RELEASE}"
  git push ${USER_REMOTE_URL}

  if [[ -z `which hub` ]]; then
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
  if [[ -z `which hub` ]]; then
    hub pull-request -m "Publish ${RELEASE} release" -h ${USER_GITHUB_ID}:updates_release_${RELEASE} -b apache:release-docs
  else
    echo "Without hub, you need to create PR manually."
  fi

  echo "Finished v${RELEASE}-RC${RC_NUM} creation."
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_JAVA_DOC}
  rm -rf ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_PYTHON_DOC}
fi

echo "===========Please Review All Items in the Checklist=========="
echo "1. Maven artifacts deployed to https://repository.apache.org/content/repositories/"
echo "2. Source distribution deployed to https://dist.apache.org/repos/dist/dev/beam/${RELEASE}"
echo "3. Website pull request published the Java API reference manual the Python API reference manual."

echo "==============Things Needed To Be Done Manually=============="
echo "1.You need to update website updates PR with a new commit: "
echo "  - cd ~/${LOCAL_WEBSITE_UPDATE_DIR}/${LOCAL_WEBSITE_REPO}/${WEBSITE_ROOT_DIR}"
echo "  - git checkout updates_release_${RELEASE}"
echo "  - Add new release into src/get-started/downloads.md "
echo "  - commit your changes"
echo "2.You need to update website updates PR with another commit: src/get-started/downloads.md"
echo "  - add new release download links like commit: "
echo "    https://github.com/apache/beam-site/commit/29394625ce54f0c5584c3db730d3eb6bf365a80c#diff-abdcc989e94369c2324cf64b66659eda"
echo "  - update last release download links from release to archive like commit: "
echo "    https://github.com/apache/beam-site/commit/6b9bdb31324d5c0250a79224507da0ea7ae8ccbf#diff-abdcc989e94369c2324cf64b66659eda"
echo "3.Start the review-and-vote thread on the dev@ mailing list."
