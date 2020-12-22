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

# test_flink_uber_jar.sh tests the Python FlinkRunner class.

set -e
set -v

SAVE_MAIN_SESSION=0

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --flink_job_server_jar)
        FLINK_JOB_SERVER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --flink_mini_cluster_jar)
        FLINK_MINI_CLUSTER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --env_dir)
        ENV_DIR="$2"
        shift # past argument
        shift # past value
        ;;
    --python_root_dir)
        PYTHON_ROOT_DIR="$2"
        shift # past argument
        shift # past value
        ;;
    --python_version)
        PYTHON_VERSION="$2"
        shift # past argument
        shift # past value
        ;;
    --python_container_image)
        PYTHON_CONTAINER_IMAGE="$2"
        shift # past argument
        shift # past value
        ;;
    --save_main_session)
        SAVE_MAIN_SESSION=1
        shift # past value
        ;;
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
esac
done


# Go to the root of the repository
cd "$(git rev-parse --show-toplevel)"

# Verify docker command exists
command -v docker
docker -v

# Verify container has already been built
docker images --format "{{.Repository}}:{{.Tag}}" | grep "$PYTHON_CONTAINER_IMAGE"

# Set up Python environment
virtualenv -p "python$PYTHON_VERSION" "$ENV_DIR"
. $ENV_DIR/bin/activate
pip install --retries 10 -e "$PYTHON_ROOT_DIR"

# Hacky python script to find a free port. Note there is a small chance the chosen port could
# get taken before being claimed.
SOCKET_SCRIPT="
from __future__ import print_function
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 0))
print(s.getsockname()[1])
s.close()
"
FLINK_PORT=$(python -c "$SOCKET_SCRIPT")

echo "Starting Flink mini cluster listening on port $FLINK_PORT"
java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -jar "$FLINK_MINI_CLUSTER_JAR" --rest-port "$FLINK_PORT" --rest-bind-address localhost &

PIPELINE_PY="
import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create
from apache_beam.transforms import Map

logging.basicConfig(level=logging.INFO)

pipeline_options = PipelineOptions()
pipeline = beam.Pipeline(options=pipeline_options)

if pipeline_options.view_as(SetupOptions).save_main_session:
  # To test that our main session is getting plumbed through artifact staging
  # correctly, create a global variable. If the main session is not plumbed
  # through properly, global_var will be undefined and the pipeline will fail.
  global_var = 1
  pcoll = (pipeline
           | Create([0, 1, 2])
           | Map(lambda x: x + global_var))
else:
  pcoll = (pipeline
           | Create([0, 1, 2])
           | Map(lambda x: x + 1))

assert_that(pcoll, equal_to([1, 2, 3]))

result = pipeline.run()
result.wait_until_finish()
"

if [[ "$SAVE_MAIN_SESSION" -eq 0 ]]; then
  (python -c "$PIPELINE_PY" \
    --runner FlinkRunner \
    --flink_job_server_jar "$FLINK_JOB_SERVER_JAR" \
    --parallelism 1 \
    --environment_type DOCKER \
    --environment_options "docker_container_image=$PYTHON_CONTAINER_IMAGE" \
    --flink_master "localhost:$FLINK_PORT" \
    --flink_submit_uber_jar \
  ) || TEST_EXIT_CODE=$? # don't fail fast here; clean up before exiting
else
  (python -c "$PIPELINE_PY" \
    --runner FlinkRunner \
    --flink_job_server_jar "$FLINK_JOB_SERVER_JAR" \
    --parallelism 1 \
    --environment_type DOCKER \
    --environment_options "docker_container_image=$PYTHON_CONTAINER_IMAGE" \
    --flink_master "localhost:$FLINK_PORT" \
    --flink_submit_uber_jar \
    --save_main_session
  ) || TEST_EXIT_CODE=$? # don't fail fast here; clean up before exiting
fi

kill %1 || echo "Failed to shut down Flink mini cluster"

rm -rf "$ENV_DIR"

if [[ "$TEST_EXIT_CODE" -eq 0 ]]; then
  echo ">>> SUCCESS"
else
  echo ">>> FAILURE"
fi
exit $TEST_EXIT_CODE
