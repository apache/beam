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

read -r -d '' USAGE <<END
Usage: run_expansion_services.sh (start|stop) [options]
Options:
  --group_id [unique id for stop services later]
  --transform_service_launcher_jar [path to the transform service launcher jar]
  --external_port [external port exposed by the transform service]
  --start [command to start the transform service for the given group_id]
  --stop [command to stop the transform service for the given group_id]
END

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --group_id)
      GROUP_ID="$2"
      shift
      shift
      ;;
    --transform_service_launcher_jar)
      TRANSFORM_SERVICE_LAUNCHER_JAR="$2"
      shift
      shift
      ;;
    --external_port)
      EXTERNAL_PORT="$2"
      shift
      shift
      ;;
    --beam_version)
      BEAM_VERSION_JAR="$2"
      BEAM_VERSION_DOCKER=${BEAM_VERSION_JAR/-SNAPSHOT/.dev}
      shift
      shift
      ;;
    start)
      STARTSTOP="$1"
      shift
      ;;
    stop)
      STARTSTOP="$1"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "$USAGE"
      exit 1
      ;;
  esac
done

FILE_BASE="beam-transform-service"
if [ -n "$GROUP_ID" ]; then
  FILE_BASE="$FILE_BASE-$GROUP_ID"
fi

TEMP_DIR=/tmp

case $STARTSTOP in
  start)
    echo "Starting the transform service for project $GROUP_ID at port $EXTERNAL_PORT for Beam version $BEAM_VERSION_DOCKER transform service startup jar is $TRANSFORM_SERVICE_LAUNCHER_JAR"
    java -jar $TRANSFORM_SERVICE_LAUNCHER_JAR --project_name $GROUP_ID --port $EXTERNAL_PORT --beam_version $BEAM_VERSION_DOCKER --command up  >$TEMP_DIR/$FILE_BASE-java1.log 2>&1 </dev/null
    ;;
  stop)
    echo "Stopping the transform service for project $GROUP_ID at port $EXTERNAL_PORT for Beam version $BEAM_VERSION_DOCKER  transform service startup jar is $TRANSFORM_SERVICE_LAUNCHER_JAR"
    java -jar $TRANSFORM_SERVICE_LAUNCHER_JAR --project_name $GROUP_ID --port $EXTERNAL_PORT --beam_version $BEAM_VERSION_DOCKER --command down  >$TEMP_DIR/$FILE_BASE-java2.log 2>&1 </dev/null
    TRANSFORM_SERVICE_TEMP_DIR=$TEMP_DIR/$GROUP_ID
    if [[ -d ${TRANSFORM_SERVICE_TEMP_DIR} ]]; then
      echo "Removing transform service temporary directory $TRANSFORM_SERVICE_TEMP_DIR"
      rm -rf ${TRANSFORM_SERVICE_TEMP_DIR}
    fi
    ;;
esac
