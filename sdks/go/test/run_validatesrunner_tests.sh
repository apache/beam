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
# services needed, such as job services or expansion services.
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
#    --tests -> A space-seperated list of targets for "go test", written with
#        beam/sdks/go as the working directory. Defaults to all packages in the
#        integration and regression directories.
#    --timeout -> Timeout for the go test command, on a per-package level.
#    --simultaneous -> Number of simultaneous packages to test.
#        Controls the -p flag for the go test command.
#        Not used for Flink, Spark, or Samza runners.  Defaults to 3 otherwise.
#    --endpoint -> An endpoint for an existing job server outside the script.
#        If present, job server jar flags are ignored.
#    --test_expansion_jar -> Filepath to jar for an expansion service, for
#        runners that support cross-language. The test expansion service is one
#        that can expand test-only cross-language transforms.
#    --test_expansion_addr -> An endpoint for an existing test expansion service
#        outside the script. If present, --test_expansion_jar is ignored.
#    --io_expansion_jar -> Filepath to jar for an expansion service, for
#        runners that support cross-language. The IO expansion service is one
#        that can expand cross-language transforms for Beam IOs.
#    --io_expansion_addr -> An endpoint for an existing expansion service
#        outside the script. If present, --io_expansion_jar is ignored.
#    --sdk_overrides -> Only needed if performing cross-lanaguage tests with
#        a staged SDK harness container. Note for Dataflow: Using this flag
#        prevents the script from creating and staging a container.
#    --pipeline_opts -> Appends additional pipeline options to the test command,
#        in addition to those already added by this script.
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

set -e
trap '! [[ "$BASH_COMMAND" =~ ^(echo|read|if|ARGS|shift|SOCKET_SCRIPT|\[\[) ]] && \
cmd=`eval echo "$BASH_COMMAND" 2>/dev/null` && echo "\$ $cmd"' DEBUG

# Resolve current directory
CURRENT_DIRECTORY=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Default test targets.
TESTS="./test/integration/... ./test/regression"

#TESTS="./test/integration/xlang"

# Default runner.
RUNNER=portable

# Default timeout. This timeout is applied per-package, as tests in different
# packages are executed in parallel.
TIMEOUT=3h

# Default limit on simultaneous test binaries/packages being executed.
SIMULTANEOUS=3

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests
GCS_SUBFOLDER="test$RANDOM"

# Project for the container and integration test
PROJECT=apache-beam-testing
DATAFLOW_PROJECT=apache-beam-testing
REGION=us-central1

# Set up trap to close any running background processes when script ends.
exit_background_processes () {
  if [[ ! -z "$JOBSERVER_PID" ]]; then
    kill -9 $JOBSERVER_PID || true
  fi
  if [[ ! -z "$TEST_EXPANSION_PID" ]]; then
    kill -9 $TEST_EXPANSION_PID
  fi
  if [[ ! -z "$IO_EXPANSION_PID" ]]; then
    kill -9 $IO_EXPANSION_PID
  fi
  if [[ ! -z "$SCHEMAIO_EXPANSION_PID" ]]; then
    kill -9 $SCHEMAIO_EXPANSION_PID
  fi
  if [[ ! -z "$DEBEZIUMIO_EXPANSION_PID" ]]; then
    kill -9 $DEBEZIUMIO_EXPANSION_PID
  fi
}
trap exit_background_processes SIGINT SIGTERM EXIT

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --tests)
        TESTS="$2"
        shift # past argument
        shift # past value
        ;;
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
    --simultaneous)
        SIMULTANEOUS="$2"
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
    --flink_job_server_jar)
        FLINK_JOB_SERVER_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --samza_job_server_jar)
        SAMZA_JOB_SERVER_JAR="$2"
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
    --test_expansion_jar)
        TEST_EXPANSION_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --test_expansion_addr)
        TEST_EXPANSION_ADDR="$2"
        shift # past argument
        shift # past value
        ;;
    --io_expansion_jar)
        IO_EXPANSION_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --io_expansion_addr)
        IO_EXPANSION_ADDR="$2"
        shift # past argument
        shift # past value
        ;;
    --schemaio_expansion_jar)
        SCHEMAIO_EXPANSION_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --schemaio_expansion_addr)
        SCHEMAIO_EXPANSION_ADDR="$2"
        shift # past argument
        shift # past value
        ;;
    --debeziumio_expansion_jar)
        DEBEZIUMIO_EXPANSION_JAR="$2"
        shift # past argument
        shift # past value
        ;;
    --debeziumio_expansion_addr)
        DEBEZIUMIO_EXPANSION_ADDR="$2"
        shift # past argument
        shift # past value
        ;;
    --sdk_overrides)
        SDK_OVERRIDES="$2"
        shift # past argument
        shift # past value
        ;;
    --pipeline_opts)
        PIPELINE_OPTS="$2"
        shift # past argument
        shift # past value
        ;;
    --java11_home)
        JAVA11_HOME="$2"
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

TMPDIR=$(mktemp -d)

# Set up environment based on runner.
if [[ "$RUNNER" == "flink" || "$RUNNER" == "spark" || "$RUNNER" == "samza" || "$RUNNER" == "portable" || "$RUNNER" == "prism" ]]; then
  if [[ -z "$ENDPOINT" ]]; then
    JOB_PORT=$(python3 -c "$SOCKET_SCRIPT")
    ENDPOINT="localhost:$JOB_PORT"
    echo "No endpoint specified; starting a new $RUNNER job server on $ENDPOINT"
    if [[ "$RUNNER" == "flink" ]]; then
      java \
          -jar $FLINK_JOB_SERVER_JAR \
          --flink-master [local] \
          --flink-conf-dir $CURRENT_DIRECTORY/../../../runners/flink/src/test/resources \
          --job-port $JOB_PORT \
          --expansion-port 0 \
          --artifact-port 0 &
    elif [[ "$RUNNER" == "samza" ]]; then
      java \
          -jar $SAMZA_JOB_SERVER_JAR \
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
    elif [[ "$RUNNER" == "portable" ]]; then
      python3 \
          -m apache_beam.runners.portability.local_job_service_main \
          --port $JOB_PORT &
    elif [[ "$RUNNER" == "prism" ]]; then
      PRISMBIN=$TMPDIR/prismbin
      cd sdks
      ./go/run_with_go_version.sh build -o $PRISMBIN go/cmd/prism/*.go
      $PRISMBIN \
      --serve_http=false \
      --job_port $JOB_PORT &
      cd ..
    else
      echo "Unknown runner: $RUNNER"
      exit 1;
    fi
    JOBSERVER_PID=$!
  fi
fi

if [[ "$RUNNER" != "direct" ]]; then
  if [[ -z "$TEST_EXPANSION_ADDR" && -n "$TEST_EXPANSION_JAR" ]]; then
    EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
    TEST_EXPANSION_ADDR="localhost:$EXPANSION_PORT"
    echo "No test expansion address specified; starting a new test expansion server on $TEST_EXPANSION_ADDR"
    java -jar $TEST_EXPANSION_JAR $EXPANSION_PORT &
    TEST_EXPANSION_PID=$!
  fi
  if [[ -z "$IO_EXPANSION_ADDR" && -n "$IO_EXPANSION_JAR" ]]; then
    EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
    IO_EXPANSION_ADDR="localhost:$EXPANSION_PORT"
    echo "No IO expansion address specified; starting a new IO expansion server on $IO_EXPANSION_ADDR"
    java -jar $IO_EXPANSION_JAR $EXPANSION_PORT &
    IO_EXPANSION_PID=$!
  fi
  if [[ -z "$SCHEMAIO_EXPANSION_ADDR" && -n "$SCHEMAIO_EXPANSION_JAR" ]]; then
      EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
      SCHEMAIO_EXPANSION_ADDR="localhost:$EXPANSION_PORT"
      echo "No SchemaIO expansion address specified; starting a new SchemaIO expansion server on $SCHEMAIO_EXPANSION_ADDR"
      java -jar $SCHEMAIO_EXPANSION_JAR $EXPANSION_PORT &
      SCHEMAIO_EXPANSION_PID=$!
  fi
  if [[ -z "$DEBEZIUMIO_EXPANSION_ADDR" && -n "$DEBEZIUMIO_EXPANSION_JAR" ]]; then
      EXPANSION_PORT=$(python3 -c "$SOCKET_SCRIPT")
      DEBEZIUMIO_EXPANSION_ADDR="localhost:$EXPANSION_PORT"
      echo "No DebeziumIO expansion address specified; starting a new DebeziumIO expansion server on $DEBEZIUMIO_EXPANSION_ADDR"
      java -jar $DEBEZIUMIO_EXPANSION_JAR $EXPANSION_PORT &
      DEBEZIUMIO_EXPANSION_PID=$!
  fi
fi

# Disable parallelism on runners that don't support it.
if [[ "$RUNNER" == "flink" || "$RUNNER" == "spark" || "$RUNNER" == "samza" ]]; then
  SIMULTANEOUS=1
fi

if [[ "$RUNNER" == "dataflow" ]]; then
  # Verify docker and gcloud commands exist
  command -v docker
  docker -v
  command -v gcloud
  gcloud --version

  # ensure gcloud is version 186 or above
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

  # TODO(https://github.com/apache/beam/issues/27674): remove this branch once the jenkins VM can build multiarch, or jenkins is deprecated.
  if [[ "$USER" == "jenkins" ]]; then
    ./gradlew :sdks:go:container:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG

    # Verify it exists
    docker images | grep $TAG

    # Push the container
    gcloud docker -- push $CONTAINER:$TAG
  else
    ./gradlew :sdks:go:container:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$TAG -Pcontainer-architecture-list=arm64,amd64 -Ppush-containers
  fi

  if [[ -n "$TEST_EXPANSION_ADDR" || -n "$IO_EXPANSION_ADDR" || -n "$SCHEMAIO_EXPANSION_ADDR" || -n "$DEBEZIUMIO_EXPANSION_ADDR" ]]; then
    ARGS="$ARGS --experiments=use_portable_job_submission"

    if [[ -z "$SDK_OVERRIDES" ]]; then
      # Build the java container for cross-language
      JAVA_TAG=$(date +%Y%m%d-%H%M%S)
      JAVA_CONTAINER=us.gcr.io/$PROJECT/$USER/beam_java11_sdk
      echo "Using container $JAVA_CONTAINER for cross-language java transforms"
      ./gradlew :sdks:java:container:java11:docker -Pdocker-repository-root=us.gcr.io/$PROJECT/$USER -Pdocker-tag=$JAVA_TAG -Pjava11Home=$JAVA11_HOME

      # Verify it exists
      docker images | grep $JAVA_TAG

      # Push the container
      gcloud docker -- push $JAVA_CONTAINER:$JAVA_TAG

      SDK_OVERRIDES=".*java.*,$JAVA_CONTAINER:$JAVA_TAG"
    fi
  fi
else
  TAG=20240911-213809
  CONTAINER=us.gcr.io/apache-beam-testing/github-actions/beam_go_sdk
fi

# The go test flag -p dictates the number of simultaneous test binaries running tests.
# Note that --parallel indicates within a test binary level of parallism.
ARGS="$ARGS -p $SIMULTANEOUS"

# Assemble test arguments and pipeline options.
ARGS="$ARGS -v"
ARGS="$ARGS -timeout $TIMEOUT"
ARGS="$ARGS --runner=$RUNNER"
ARGS="$ARGS --project=$DATAFLOW_PROJECT"
ARGS="$ARGS --region=$REGION"
ARGS="$ARGS --environment_type=DOCKER"
ARGS="$ARGS --environment_config=$CONTAINER:$TAG"
ARGS="$ARGS --staging_location=$GCS_LOCATION/staging-validatesrunner-test/$GCS_SUBFOLDER"
ARGS="$ARGS --temp_location=$GCS_LOCATION/temp-validatesrunner-test/$GCS_SUBFOLDER"
ARGS="$ARGS --endpoint=$ENDPOINT"
if [[ -n "$TEST_EXPANSION_ADDR" ]]; then
  ARGS="$ARGS --test_expansion_addr=$TEST_EXPANSION_ADDR"
fi
if [[ -n "$IO_EXPANSION_ADDR" ]]; then
  ARGS="$ARGS --io_expansion_addr=$IO_EXPANSION_ADDR"
fi
if [[ -n "$SCHEMAIO_EXPANSION_ADDR" ]]; then
  ARGS="$ARGS --schemaio_expansion_addr=$SCHEMAIO_EXPANSION_ADDR"
fi
if [[ -n "$DEBEZIUMIO_EXPANSION_ADDR" ]]; then
  ARGS="$ARGS --debeziumio_expansion_addr=$DEBEZIUMIO_EXPANSION_ADDR"
fi
if [[ -n "$SDK_OVERRIDES" ]]; then
  OVERRIDE=--sdk_harness_container_image_override="$SDK_OVERRIDES"
  ARGS="$ARGS $OVERRIDE"
fi
ARGS="$ARGS $PIPELINE_OPTS"

cd sdks/go
echo ">>> RUNNING $RUNNER integration tests with pipeline options: $ARGS"
./run_with_go_version.sh test -v $TESTS $ARGS 1>&2 \
    || TEST_EXIT_CODE=$? # don't fail fast here; clean up environment before exiting
cd ../..

if [[ "$RUNNER" == "dataflow" ]]; then
  # Delete the container locally and remotely
  docker rmi $CONTAINER:$TAG || echo "Built container image was not removed. Possibly, it was not not saved locally."
  # Note: we don't delete the multi-arch containers here because this command only deletes the manifest list with the tag,
  # the associated container images can't be deleted because they are not tagged. However, multi-arch containers that are
  # older than 6 weeks old are deleted by stale_dataflow_prebuilt_image_cleaner.sh that runs daily.
  if [[ "$USER" == "jenkins" ]]; then
    gcloud --quiet container images delete $CONTAINER:$TAG || echo "Failed to delete container"
  fi
  if [[ -n "$TEST_EXPANSION_ADDR" || -n "$IO_EXPANSION_ADDR" || -n "$SCHEMAIO_EXPANSION_ADDR" || -n "$DEBEZIUMIO_EXPANSION_ADDR" ]]; then
    # Delete the java cross-language container locally and remotely
    docker rmi $JAVA_CONTAINER:$JAVA_TAG || echo "Failed to remove container"
    gcloud --quiet container images delete $JAVA_CONTAINER:$JAVA_TAG || echo "Failed to delete container"
  fi
fi

# Clean up tempdir
rm -rf $TMPDIR

exit $TEST_EXIT_CODE
