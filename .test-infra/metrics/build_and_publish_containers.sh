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

#
# This script will:
# * build containers for beam metrics
# * publish containers if first parameter is provided
#
# Usage:
# "build_and_publish_container.sh [<should_publish_containers>] [<container_tag_name>]"
#

if [ -z "${PROJECT_ID}" ]; then
  echo "PROJECT_ID not set, trying to automatically get it from gcloud config."
  PROJECT_ID=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
  echo "Using ${PROJECT_ID}."
fi

echo
echo ===========Start==========
DO_PUSH=$1
CONTAINER_VERSION_NAME=${2:-latest}

echo
echo ===========Building containers==========
docker build -t gcr.io/${PROJECT_ID}/beamgrafana:$CONTAINER_VERSION_NAME ./grafana
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjenkins:$CONTAINER_VERSION_NAME ./sync/jenkins
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjira:$CONTAINER_VERSION_NAME ./sync/jira
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncgithub:$CONTAINER_VERSION_NAME ./sync/github

if [ "$DO_PUSH" = true ]; then
  echo
  echo ===========Publishing containers==========
  docker push gcr.io/${PROJECT_ID}/beamgrafana:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncjenkins:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncjira:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncgithub:$CONTAINER_VERSION_NAME
fi
