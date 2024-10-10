#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e               # exit on error

cd "$(dirname "$0")" # connect to root

DOCKER_DIR=dev-support/docker
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

CONTAINER_NAME=beam-dev-${USER}-$$

if [ ! -x "$(command -v docker)" ] ; then
  echo "Unable to locate docker"
  echo "This Beam Build environment is based upon docker so you must install it first."
  exit 1;
fi

docker build -t beam-build -f $DOCKER_FILE $DOCKER_DIR

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

if [ "$(uname -s)" = "Darwin" ]; then
  GROUP_ID=100
  if (dscl . -read /Groups/docker 2>/dev/null); then 
      DOCKER_GROUP_ID=$(dscl . -read /Groups/docker| awk '($1 == "PrimaryGroupID:") { print $2 }')
    else
      # if Docker post-install steps to manage as non-root user not performed - will use dummy gid
      DOCKER_GROUP_ID=1000
      echo "`tput setaf 3`Please take a look post-install steps, to manage Docker as non-root user"
      echo "`tput setaf 2`https://docs.docker.com/engine/install/linux-postinstall`tput sgr0`"
  fi
fi

if [ "$(uname -s)" = "Linux" ]; then
  DOCKER_GROUP_ID=$(getent group docker | cut -d':' -f3)
  GROUP_ID=$(id -g "${USER_NAME}")
  # man docker-run
  # When using SELinux, mounted directories may not be accessible
  # to the container. To work around this, with Docker prior to 1.7
  # one needs to run the "chcon -Rt svirt_sandbox_file_t" command on
  # the directories. With Docker 1.7 and later the z mount option
  # does this automatically.
  if command -v selinuxenabled >/dev/null && selinuxenabled; then
    DCKR_VER=$(docker -v|
    awk '$1 == "Docker" && $2 == "version" {split($3,ver,".");print ver[1]"."ver[2]}')
    DCKR_MAJ=${DCKR_VER%.*}
    DCKR_MIN=${DCKR_VER#*.}
    if [ "${DCKR_MAJ}" -eq 1 ] && [ "${DCKR_MIN}" -ge 7 ] ||
        [ "${DCKR_MAJ}" -gt 1 ]; then
      V_OPTS=:z
    else
      for d in "${PWD}" "${HOME}/.m2"; do
        ctx=$(stat --printf='%C' "$d"|cut -d':' -f3)
        if [ "$ctx" != svirt_sandbox_file_t ] && [ "$ctx" != container_file_t ]; then
          printf 'INFO: SELinux is enabled.\n'
          printf '\tMounted %s may not be accessible to the container.\n' "$d"
          printf 'INFO: If so, on the host, run the following command:\n'
          printf '\t# chcon -Rt svirt_sandbox_file_t %s\n' "$d"
        fi
      done
    fi
  fi
fi

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

docker build -t "beam-build-${USER_ID}" - <<UserSpecificDocker
FROM beam-build
RUN rm -f /var/log/faillog /var/log/lastlog
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN groupmod -g ${DOCKER_GROUP_ID} docker
RUN useradd -g ${GROUP_ID} -G docker -u ${USER_ID} -k /root -m ${USER_NAME} -d "${DOCKER_HOME_DIR}"
RUN echo "${USER_NAME} ALL=NOPASSWD: ALL" > "/etc/sudoers.d/beam-build-${USER_ID}"
ENV HOME "${DOCKER_HOME_DIR}"
ENV GOPATH ${DOCKER_HOME_DIR}/beam/sdks/go/examples/.gogradle/project_gopath
# This next command still runs as root causing the ~/.cache/go-build to be owned by root
RUN go install github.com/linkedin/goavro/v2
RUN chown -R ${USER_NAME}:${GROUP_ID} ${DOCKER_HOME_DIR}/.cache
UserSpecificDocker

echo ""
echo "Docker image build completed."
echo "=============================================================================================="
echo ""

# If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

DOCKER_SOCKET_MOUNT=""
if [ -S /var/run/docker.sock ];
then
  DOCKER_SOCKET_MOUNT="-v /var/run/docker.sock:/var/run/docker.sock${V_OPTS:-}"
  echo "Enabling Docker support with the docker build environment."
else
  echo "There is NO Docker support with the docker build environment."
fi

COMMAND=( "$@" )
if [ $# -eq 0 ];
then
  COMMAND=( "bash" )
fi

[ -d "${HOME}/.beam_docker_build_env/.gradle" ] || mkdir -p "${HOME}/.beam_docker_build_env/.gradle"

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
docker run --rm=true ${DOCKER_INTERACTIVE_RUN}                         \
           --name "${CONTAINER_NAME}"                                  \
           --network=host                                              \
           -v "${HOME}/.m2:${DOCKER_HOME_DIR}/.m2${V_OPTS:-}"          \
           -v "${HOME}/.gnupg:${DOCKER_HOME_DIR}/.gnupg${V_OPTS:-}"    \
           -v "${HOME}/.beam_docker_build_env/.gradle:${DOCKER_HOME_DIR}/.gradle${V_OPTS:-}"  \
           -v "${PWD}:${DOCKER_HOME_DIR}/beam${V_OPTS:-}"              \
           -w "${DOCKER_HOME_DIR}/beam"                                \
           ${DOCKER_SOCKET_MOUNT}                                      \
           -u "${USER_ID}"                                             \
           "beam-build-${USER_ID}" "${COMMAND[@]}"
