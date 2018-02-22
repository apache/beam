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
# Runs Python HDFS integration tests.
#
# Requires docker, docker-compose to be installed.

set -e -u -x

# Setup context directory.
TEST_DIR=$(dirname $0)
ROOT_DIR=${TEST_DIR}/../../../../..
CONTEXT_DIR=${ROOT_DIR}/build/hdfs_integration
rm -r ${CONTEXT_DIR} || true
mkdir -p ${CONTEXT_DIR}/sdks
cp ${TEST_DIR}/* ${CONTEXT_DIR}/
cp -r ${ROOT_DIR}/sdks/python ${CONTEXT_DIR}/sdks/
cp -r ${ROOT_DIR}/model ${CONTEXT_DIR}/

# Use a unique name to allow concurrent runs on the same machine.
PROJECT_NAME=$(echo hdfs_IT-${BUILD_TAG:-non-jenkins})

cd ${CONTEXT_DIR}
time docker-compose -p ${PROJECT_NAME} build
time docker-compose -p ${PROJECT_NAME} up --exit-code-from test \
    --abort-on-container-exit
