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
# services needed, such as job services or expansion services. The following
# runners are supported, and selected via a flag:
#
# --runner {portable|direct|flink} (default: portable)
#  Select which runner to execute tests on. This flag also determines which
#  services to start up and which tests may be skipped.
#    direct   - Go SDK Direct Runner
#    portable - (default) Python Portable Runner (aka. Reference Runner or FnAPI Runner)
#    flink    - Java Flink Runner (local mode)
#    spark    - Java Spark Runner (local mode)
#
# --flink_job_server_jar -> Filepath to jar, used if runner is Flink.
# --spark_job_server_jar -> Filepath to jar, used if runner is Spark.
# --endpoint -> Replaces jar filepath with existing job server endpoint.
#
# --expansion_service_jar -> Filepath to jar for expansion service.
# --expansion_addr -> Replaces jar filepath with existing expansion service endpoint.
#
# Execute from the root of the repository. This script requires that necessary
# services can be built from the repository.

set -e
set -v

RUNNER=portable

# Set up trap to close any running background processes when script ends.
exit_background_processes () {
  if [[ -n "$JOBSERVER_PID" ]]; then
    kill -s SIGKILL $JOBSERVER_PID
  fi
  if [[ -n "$EXPANSION_PID" ]]; then
    kill -s SIGKILL $EXPANSION_PID
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
ARGS=--runner=$RUNNER
if [[ "$RUNNER" == "flink" || "$RUNNER" == "spark" || "$RUNNER" == "portable" ]]; then
  if [[ -z "$ENDPOINT" ]]; then
    JOB_PORT=$(python -c "$SOCKET_SCRIPT")
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
      ARGS="$ARGS -p 1" # Spark runner fails if jobs are run concurrently.
    elif [[ "$RUNNER" == "portable" ]]; then
      python \
          -m apache_beam.runners.portability.local_job_service_main \
          --port $JOB_PORT &
    else
      echo "Unknown runner: $RUNNER"
      exit 1;
    fi
    JOBSERVER_PID=$!
  fi

  if [[ -z "$EXPANSION_ADDR" ]]; then
    EXPANSION_PORT=$(python -c "$SOCKET_SCRIPT")
    EXPANSION_ADDR="localhost:$EXPANSION_PORT"
    echo "No expansion address specified; starting a new expansion server on $EXPANSION_ADDR"
    java -jar $EXPANSION_SERVICE_JAR $EXPANSION_PORT &
    EXPANSION_PID=$!
  fi

  ARGS="$ARGS --endpoint=$ENDPOINT --expansion_addr=$EXPANSION_ADDR"
fi

echo ">>> RUNNING $RUNNER VALIDATESRUNNER TESTS"
go test -v ./sdks/go/test/integration/... $ARGS

exit_background_processes
