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
Usage: run_kafka_services.sh (start|stop)
END

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
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

KAFKA_VERSION=2.2.0
SCALA_VERSION=2.12
KAFKA_URL="https://www-us.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz"

TEMP_DIR=/tmp/local-kafka
mkdir -p $TEMP_DIR

case $STARTSTOP in
  start)
    curl -s -o $TEMP_DIR/kafka.tgz $KAFKA_URL
    tar xzf $TEMP_DIR/kafka.tgz -C $TEMP_DIR
    cd $TEMP_DIR/kafka_$SCALA_VERSION-$KAFKA_VERSION
    if [ -f config/server.properties ]; then
      echo -e "\ndelete.topic.enable=true" >> config/server.properties
    fi
    echo "Starting local Zookeeper server."
    bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
    sleep 3
    echo "Starting local Kafka server."
    bin/kafka-server-start.sh -daemon config/server.properties
    sleep 3
    echo "Cleanup beam-kafkaio-test topic."
    bin/kafka-topics.sh --zookeeper localhost:2181 --topic beam-kafkaio-test --delete --if-exists
    ;;
  stop)
    cd $TEMP_DIR/kafka_$SCALA_VERSION-$KAFKA_VERSION
    echo "Stopping local Kafka server."
    bin/kafka-server-stop.sh
    sleep 3
    echo "Stopping local Zookeeper server."
    bin/zookeeper-server-stop.sh
    ;;
esac
