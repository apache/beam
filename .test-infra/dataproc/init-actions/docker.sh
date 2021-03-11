#!/usr/bin/env bash
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
#    This script installs and configures Docker on Google Cloud Dataproc Cluster.
#    For information about which software components (and their version) are included
#    in Cloud Dataproc clusters, see the Cloud Dataproc Image Version information:
#    https://cloud.google.com/dataproc/concepts/dataproc-versions
#
#    This file originated from:
#    https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/docker/docker.sh
#    (last commit: 6477e6067cc7a08de165117778a251ac2ed6a62f)
#
set -euxo pipefail

readonly OS_ID=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_CODE=$(lsb_release -cs)
# TODO: Allow this to be configured by metadata.
readonly DOCKER_VERSION="18.06.0~ce~3-0~${OS_ID}"
readonly CREDENTIAL_HELPER_VERSION='2.0.2'


function is_master() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "$role" == 'Master' ]] ; then
    true
  else
    false
  fi
}

function get_docker_gpg() {
  curl -fsSL https://download.docker.com/linux/${OS_ID}/gpg
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)) ; do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_docker() {
  update_apt_get
  apt-get install -y apt-transport-https ca-certificates curl gnupg2
  get_docker_gpg | apt-key add -
  echo "deb [arch=amd64] https://download.docker.com/linux/${OS_ID} ${OS_CODE} stable" >/etc/apt/sources.list.d/docker.list
  update_apt_get
  apt-get install -y docker-ce="${DOCKER_VERSION}"
}

function configure_gcr() {
  # this standalone method is recommended here:
  # https://cloud.google.com/container-registry/docs/advanced-authentication#standalone_docker_credential_helper
  curl -fsSL --retry 10 "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v${CREDENTIAL_HELPER_VERSION}/docker-credential-gcr_linux_amd64-${CREDENTIAL_HELPER_VERSION}.tar.gz" \
    | tar xz --to-stdout ./docker-credential-gcr \
    > /usr/local/bin/docker-credential-gcr && chmod +x /usr/local/bin/docker-credential-gcr

  # this command configures docker on a per-user basis. Therefore we configure
  # the root user, as well as the yarn user which is part of the docker group.
  # If additional users are added to the docker group later, this command will
  # need to be run for them as well.
  docker-credential-gcr configure-docker
  su yarn --command "docker-credential-gcr configure-docker"
}

function configure_docker() {
  # The installation package should create `docker` group.
  usermod -aG docker yarn
  # configure docker to use Google Cloud Registry
  configure_gcr

  systemctl enable docker
  # Restart YARN daemons to pick up new group without restarting nodes.
  if is_master ; then
    systemctl restart hadoop-yarn-resourcemanager
  else
    systemctl restart hadoop-yarn-nodemanager
  fi
}

function main() {
  install_docker
  configure_docker
}

main "$@"
