#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Kill db emulator
DATASTORE_FULL_ADDRESS="${DATASTORE_EMULATOR_HOST:-"127.0.0.1:8888"}"
DATASTORE_PORT="${DATASTORE_FULL_ADDRESS##*:}"

PID=$(lsof -t -i :"${DATASTORE_PORT}" -s tcp:LISTEN)
if [ ! -z "$PID" ]; then
  echo "Stopping Datastore emulator"
  kill "$PID"
fi
