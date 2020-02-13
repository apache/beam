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

# This script will install and configure GPG key.

set -e

LOCAL_SVN_DIR=local_svn_dir
ROOT_SVN_URL=https://dist.apache.org/repos/dist/
DEV_REPO=dev
RELEASE_REPO=release
BEAM_REPO=beam

cd ~

echo "=================Checking GPG Key===================="
echo "You need a GPG key which reflects your Apache account."
echo "Do you want to generate a new GPG key associated with your Apache account? [y|N]"
read confirmation
if [[ $confirmation = "y" ]]; then
  echo "===============Generating new GPG key================"
  sudo apt-get install rng-tools
  sudo rngd -r /dev/urandom
  echo "NOTE: When creating the key, please select the type to be RSA and RSA (default), and the size to be 4096 bit long."
  gpg --full-generate-key
fi

echo "================Listing all GPG keys================="
gpg --list-keys
echo "Please copy the public key which is associated with your Apache account:"
read pub_key

echo "===========Configuring git signing key==============="
git config --global user.signingkey $pub_key
git config --list

echo "===========Adding your key into KEYS file============"
echo "It's required to append your key into KEYS file in dist.apache.org"
echo "Have you put your key in KEYS? [y|N]"
read confirmation
if [[ $confirmation != "y" ]]; then
  echo "Only PMC member can write into dist.apache.org. Are you a PMC member? [y|N]"
  read pmc_permission
  if [[ $pmc_permission != "y" ]]; then
    echo "Please ask a PMC member to help you add your key in dev@ list."
    echo "Skip adding key into dist.apache.org/KEYS file."
  else
    echo "Please input your name: "
    read name
    echo "======Starting updating KEYS file in dev repo===="
    if [[ -d ${LOCAL_SVN_DIR} ]]; then
      rm -rf ${LOCAL_SVN_DIR}
    fi
    mkdir ${LOCAL_SVN_DIR}
    cd ${LOCAL_SVN_DIR}
    svn co ${ROOT_SVN_URL}/${DEV_REPO}/${BEAM_REPO}
    cd ${BEAM_REPO}
    (gpg --list-sigs ${name} && gpg --armor --export ${name}) >> KEYS
    svn status
    echo "Please review all changes. Do you confirm to commit? [y|N]"
    read commit_confirmation
    if [[ $commit_confirmation = "y" ]]; then
      svn commit --no-auth-cache KEYS
    else
      echo "Not commit new changes into ${ROOT_SVN_URL}/${DEV_REPO}/${BEAM_REPO}${DEV_REPO}/KEYS"
    fi

    cd ~/${LOCAL_SVN_DIR}
    echo "===Starting updating KEYS file in release repo==="
    svn co ${ROOT_SVN_URL}/${RELEASE_REPO}/${BEAM_REPO}
    cd ${BEAM_REPO}
    (gpg --list-sigs ${name} && gpg --armor --export ${name}) >> KEYS
    svn status
    echo "Please review all changes. Do you confirm to commit? [y|N]"
    read commit_confirmation
    if [[ $commit_confirmation = "y" ]]; then
      svn commit --no-auth-cache KEYS
    else
      echo "Not commit new changes into ${ROOT_SVN_URL}/${DEV_REPO}/${BEAM_REPO}${RELEASE_REPO}/KEYS"
    fi

    cd ~
    rm -rf ${LOCAL_SVN_DIR}
  fi
fi

echo "================Setting up gpg agent================="
eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
export GPG_TTY=$(tty)
export GPG_AGENT_INFO
