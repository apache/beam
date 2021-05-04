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

# This script executes ValidatesRunner tests including launching any additional
# services needed, such as job services or expansion services. This script
# should be executed from the root of the Beam repository.
#
# The following runners are supported, and selected via a flag:
# --runner {portable|direct|flink} (default: portable)
#  Select which runner to execute tests on. This flag also determines which
#  services to start up and which tests may be skipped.
#    direct   - Go SDK Direct Runner
#    portable - (default) Python Portable Runner (aka. Reference Runner or FnAPI Runner)
#    flink    - Java Flink Runner (local mode)
#    spark    - Java Spark Runner (local mode)
#    dataflow - Dataflow Runner
#
# General flags:
#    --timeout -> Timeout for the go test command, on a per-package level.
#    --endpoint -> An endpoint for an existing job server outside the script.
#        If present, job server jar flags are ignored.
#    --expansion_service_jar -> Filepath to jar for expansion service, for
#        runners that support cross-language.
#    --expansion_addr -> An endpoint for an existing expansion service outside
#        the script. If present, --expansion_service_jar is ignored.
#
# Runner-specific flags:
#  Flink
#    --flink_job_server_jar -> Filepath to jar, used if runner is Flink.
#  Spark
#    --spark_job_server_jar -> Filepath to jar, used if runner is Spark.
#  Dataflow
#    --dataflow_project -> GCP project to run Dataflow jobs on.
#    --project -> Same project as --dataflow-project, but in URL format, for
#        example in the format "us.gcr.io/<project>".
#    --region -> GCP region to run Dataflow jobs on.
#    --gcs_location -> GCS URL for storing temporary files for Dataflow jobs.
#    --dataflow_worker_jar -> The Dataflow worker jar to use when running jobs.
#        If not specified, the script attempts to retrieve a previously built
#        jar from the appropriate gradle module, which may not succeed.

set -e
set -v

# Default runner.
RUNNER=portable

# Default timeout. This timeout is applied per-package, as tests in different
# packages are executed in parallel.
TIMEOUT=1h

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

# Project for the container and integration test
PROJECT=apache-beam-testing
DATAFLOW_PROJECT=apache-beam-testing
REGION=us-central1

# Set up trap to close any running background processes when script ends.
exit_background_processes () {
  if [[ ! -z "$JOBSERVER_PID" ]]; then
    kill -9 $JOBSERVER_PID || true
  fi
  if [[ ! -z "$EXPANSION_PID" ]]; then
    kill -9 $EXPANSION_PID
  fi
}
trap exit_background_processes SIGINT SIGTERM EXIT

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --runner)
        RUNNER="$2"
        shift # past argument
        shift # past value
        ;;
    --timeout)
        TIMEOUT="$2"
        shift # past argument
        shift # past value
        ;;
    --project)
        PROJECT="$2"
        shift # past argument
        shift # past value
        ;;
    --region)
        REGION="$2"
        shift # past argument
        shift # past value
        ;;
    --dataflow_project)
        DATAFLOW_PROJECT="$2"
        shift # past argument
        shift # past value
        ;;
    --gcs_location)
        GCS_LOCATION="$2"
        shift # past argument
        shift # past value
        ;;
    --dataflow_worker_jar)
        DATAFLOW_WORKER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --flink_job_server_jar)
        FLINK_JOB_SERVER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --spark_job_server_jar)
        SPARK_JOB_SERVER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --endpoint)
        ENDPOINT="$2"
        shift # past argument
        shift # past value
        ;;
    --expansion_service_jar)
        EXPANSION_SERVICE_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --expansion_addr)
        EXPANSION_ADDR="$2"
        shift # past argument
        shift # past value
        ;;
    --jenkins)
        JENKINS=true
        shift # past argument
        ;;
    *)    # unknown option
        echo "Unknown option: $1"
        exit 1
        ;;
esac
done

# Go to the root of the repository
cd $(git rev-parse --show-toplevel)

# Verify in the root of the repository
test -d sdks/go/test

# Hacky python script to find a free port. Note there is a small chance the chosen port could
# get taken before being claimed by the job server.
SOCKET_SCRIPT="
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 0))
print(s.getsockname()[1])
s.close()
"

# Set up environment based on runner.
if [[ "$RUNNER" == "dataflow" ]]; then
  if [[ -z "$DATAFLOW_WORKER_JAR" ]]; then
    DATAFLOW_WORKER_JAR=$(find $(pwd)/runners/google-cloud-dataflow-java/worker/build/libs/beam-runners-google-cloud-dataflow-java-fn-api-worker-*.jar)
  fi
  echo "Using Dataflow worker jar: $DATAFLOW_WORKER_JAR"

  if [[ -z "$EXPANSION_ADDR" && -n "$EXPANSION_SERVICE_JAR" ]]; then
    EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
    EXPANSION_ADDR="localhost:$EXPANSION_PORT"
    echo "No expansion address specified; starting a new expansion server on $EXPANSION_ADDR"
    java -jar $EXPANSION_SERVICE_JAR $EXPANSION_PORT &
    EXPANSION_PID=$!
  fi
elif [[ "$RUNNER" == "flink" || "$RUNNER" == "spark" || "$RUNNER" == "portable" ]]; then
  if [[ -z "$ENDPOINT" ]]; then
    JOB_PORT=$(python3 -c "$SOCKET_SCRIPT")
    ENDPOINT="localhost:$JOB_PORT"
    echo "No endpoint specified; starting a new $RUNNER job server on $ENDPOINT"
    if [[ "$RUNNER" == "flink" ]]; then
      java \
          -jar $FLINK_JOB_SERVER_JAR \
          --flink-master [local] \
          --job-port $JOB_PORT \
          --expansion-port 0 \
          --artifact-port 0 &
    elif [[ "$RUNNER" == "spark" ]]; then
      java \
          -jar $SPARK_JOB_SERVER_JAR \
          --spark-master-url local \
          --job-port $JOB_PORT \
          --expansion-port 0 \
          --artifact-port 0 &
      ARGS="-p 1" # Spark runner fails if jobs are run concurrently.
    elif [[ "$RUNNER" == "portable" ]]; then
      python3 \
          -m apache_beam.runners.portability.local_job_service_main \
          --port $JOB_PORT &
    else
      echo "Unknown runner: $RUNNER"
      exit 1;
    fi
    JOBSERVER_PID=$!
  fi

  if [[ -z "$EXPANSION_ADDR" && -n "$EXPANSION_SERVICE_JAR" ]]; then
    EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
    EXPANSION_ADDR="localhost:$EXPANSION_PORT"
    echo "No expansion address specified; starting a new expansion server on $EXPANSION_ADDR"
    java -jar $EXPANSION_SERVICE_JAR $EXPANSION_PORT &
    EXPANSION_PID=$!
  fi
fi

if [[ "$RUNNER" == "dataflow" ]]; then
  # Verify docker and gcloud commands exist
  command -v docker
  docker -v
  command -v gcloud
  gcloud --version

  # ensure gcloud is version 186 or above
  TMPDIR=$(mktemp -d)
  gcloud_ver=$(gcloud -v | head -1 | awk '{print $4}')
  if [[ "$gcloud_ver" < "186" ]]
  then
    pushd $TMPDIR
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-186.0.0-linux-x86_64.tar.gz --output gcloud.tar.gz
    tar xf gcloud.tar.gz
    ./google-cloud-sdk/install.sh --quiet
    . ./google-cloud-sdk/path.bash.inc
    popd
    gcloud components update --quiet || echo 'gcloud components update failed'
    gcloud -v
  fi

  # Build the container
  TAG=$(date +%Y%m%d-%H%M%S)
  CONTAINER=us.gcr.io/$PROJECT/$USER/beam_go_sdk
  echo "Using container $CONTAINER"
  ./gradlew :sdks:go:container:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG

  # Verify it exists
  docker images | grep $TAG

  # Push the container
  gcloud docker -- push $CONTAINER:$TAG

  if [[ -n "$EXPANSION_ADDR" ]]; then
    # Build the java container for cross-language
    JAVA_TAG=$(date +%Y%m%d-%H%M%S)
    JAVA_CONTAINER=us.gcr.io/$PROJECT/$USER/beam_java11_sdk
    echo "Using container $JAVA_CONTAINER for cross-language java transforms"
    ./gradlew :sdks:java:container:java11:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$JAVA_TAG

    # Verify it exists
    docker images | grep $JAVA_TAG

    # Push the container
    gcloud docker -- push $JAVA_CONTAINER:$JAVA_TAG
  fi
else
  TAG=dev
  ./gradlew :sdks:go:container:docker -Pdocker-tag=$TAG
  CONTAINER=apache/beam_go_sdk
fi

ARGS="$ARGS --timeout=$TIMEOUT"
ARGS="$ARGS --runner=$RUNNER"
ARGS="$ARGS --project=$DATAFLOW_PROJECT"
ARGS="$ARGS --region=$REGION"
ARGS="$ARGS --environment_type=DOCKER"
ARGS="$ARGS --environment_config=$CONTAINER:$TAG"
ARGS="$ARGS --staging_location=$GCS_LOCATION/staging-validatesrunner-test"
ARGS="$ARGS --temp_location=$GCS_LOCATION/temp-validatesrunner-test"
ARGS="$ARGS --dataflow_worker_jar=$DATAFLOW_WORKER_JAR"
ARGS="$ARGS --endpoint=$ENDPOINT"
OVERRIDE=--sdk_harness_container_image_override=".*java.*,$JAVA_CONTAINER:$JAVA_TAG"
ARGS="$ARGS $OVERRIDE"
if [[ -n "$EXPANSION_ADDR" ]]; then
  ARGS="$ARGS --expansion_addr=$EXPANSION_ADDR"
  if [[ "$RUNNER" == "dataflow" ]]; then
    ARGS="$ARGS --experiments=use_portable_job_submission"
  fi
fi

# Running "go test" requires some additional setup on Jenkins.
if [[ "$JENKINS" == true ]]; then
  # Copy the go repo as it is on Jenkins, to ensure we compile with the code
  # being tested.
  cd ..
  mkdir -p temp_gopath/src/github.com/apache/beam/sdks
  cp -a ./src/sdks/go ./temp_gopath/src/github.com/apache/beam/sdks
  TEMP_GOPATH=$(pwd)/temp_gopath
  cd ./src

  echo ">>> RUNNING $RUNNER VALIDATESRUNNER TESTS"
  GOPATH=$TEMP_GOPATH go test -v github.com/apache/beam/sdks/go/test/integration/... $ARGS \
      || TEST_EXIT_CODE=$? # don't fail fast here; clean up environment before exiting
else
  echo ">>> RUNNING $RUNNER VALIDATESRUNNER TESTS"
  go test -v ./sdks/go/test/integration/... $ARGS \
      || TEST_EXIT_CODE=$? # don't fail fast here; clean up environment before exiting
fi

if [[ "$RUNNER" == "dataflow" ]]; then
  # Delete the container locally and remotely
  docker rmi $CONTAINER:$TAG || echo "Failed to remove container"
  gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"

  if [[ -n "$EXPANSION_ADDR" ]]; then
    # Delete the java cross-language container locally and remotely
    docker rmi $JAVA_CONTAINER:$JAVA_TAG || echo "Failed to remove container"
    gcloud --quiet container images delete $JAVA_CONTAINER:$JAVA_TAG || echo "Failed to delete container"
  fi

  # Clean up tempdir
  rm -rf $TMPDIR
fi

exit $TEST_EXIT_CODE
