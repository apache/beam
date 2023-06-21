#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

ext_service_port="$EXT_SERVICE_PORT"
job_service_port="$JOB_SERVICE_PORT"

echo ext_service_port
echo job_service_port


echo "==> Launching the Docker daemon..."

dind dockerd --iptables=false &
while(! docker info > /dev/null 2>&1); do
    echo "==> Waiting for the Docker daemon to come online...???"
    sleep 1
done

echo "==> Docker Daemon is up and running!"

# Import pre-installed images
echo "==> Loading pre-pulled docker images!"
for file in /images/*.tar; do
echo "Loading $file"
    docker load <$file
done
rm -f -r images

docker images

usermod -aG docker appuser
su appuser 
java -jar beam-examples-multi-language.jar "$ext_service_port" --javaClassLookupAllowlistFile='*' &> es.log & \
python -m apache_beam.runners.portability.local_job_service_main -p "$job_service_port" &> js.log  &

/opt/playground/backend/server_python_backend
