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

set -e
set -v

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --job_server_jar)
        JOB_SERVER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --runner)
        RUNNER="$2"
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
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
esac
done

# Go to the root of the repository
cd $(git rev-parse --show-toplevel)

# Verify docker command exists
command -v docker
docker -v

# Verify container has already been built
docker images --format "{{.Repository}}:{{.Tag}}" | grep $PYTHON_CONTAINER_IMAGE

# Set up Python environment
virtualenv -p python$PYTHON_VERSION $ENV_DIR
. $ENV_DIR/bin/activate
pip install --retries 10 -e $PYTHON_ROOT_DIR

PIPELINE_PY="
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create
from apache_beam.transforms import Map

# To test that our main session is getting plumbed through artifact staging
# correctly, create a global variable. If the main session is not plumbed
# through properly, global_var will be undefined and the pipeline will fail.
global_var = 1

pipeline_options = PipelineOptions()
pipeline_options.view_as(SetupOptions).save_main_session = True
pipeline = beam.Pipeline(options=pipeline_options)
pcoll = (pipeline
         | Create([0, 1, 2])
         | Map(lambda x: x + global_var))
assert_that(pcoll, equal_to([1, 2, 3]))

result = pipeline.run()
result.wait_until_finish()
"

if [[ "$RUNNER" = "FlinkRunner" ]]; then
  INPUT_JAR_ARG="flink_job_server_jar"
else
  INPUT_JAR_ARG="spark_job_server_jar"
fi

# Create the jar
OUTPUT_JAR=flink-test-$(date +%Y%m%d-%H%M%S).jar
(python -c "$PIPELINE_PY" \
  --runner "$RUNNER" \
  --"$INPUT_JAR_ARG" "$JOB_SERVER_JAR" \
  --output_executable_path $OUTPUT_JAR \
  --parallelism 1 \
  --sdk_worker_parallelism 1 \
  --environment_type DOCKER \
  --environment_options "docker_container_image=$PYTHON_CONTAINER_IMAGE" \
) || TEST_EXIT_CODE=$? # don't fail fast here; clean up before exiting

if [[ "$TEST_EXIT_CODE" -eq 0 ]]; then
  # Execute the jar
  java -jar $OUTPUT_JAR || TEST_EXIT_CODE=$?
fi

rm -rf $ENV_DIR
rm -f $OUTPUT_JAR

if [[ "$TEST_EXIT_CODE" -eq 0 ]]; then
  echo ">>> SUCCESS"
else
  echo ">>> FAILURE"
fi
exit $TEST_EXIT_CODE
