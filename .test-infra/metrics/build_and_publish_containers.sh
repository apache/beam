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
# * publish containers if second parameter is provided
# * update beamgrafana-deploy.yaml with new container versions
#

if [ "$#" = 0 ]; then
  echo "build_and_publish_container.sh <container_tag_name> [<should_publish_containers>]"
  exit 0;
fi

echo
echo ===========Start==========
CONTAINER_VERSION_NAME=$1
DO_PUSH=$2

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

echo
echo ===========Updating deployment script==========
set -o xtrace
sed -i "s/\( *image: gcr.io\/${PROJECT_ID}\/beam.*:\).*$/\1$CONTAINER_VERSION_NAME/g" ./beamgrafana-deploy.yaml
set +o xtrace

