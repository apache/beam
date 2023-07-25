#!/usr/bin/env bash

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

# Check user environment variable
if [[ -z "${DATASTORE_PROJECT_ID}" ]]; then
  echo "Missing DATASTORE_PROJECT_ID environment variable" >&2
  exit 1
fi

if [[ -z "${DATASTORE_LISTEN_ADDRESS}" ]]; then
  echo "Missing DATASTORE_LISTEN_ADDRESS environment variable" >&2
  exit 1
fi

options=${options:1}
# Check for datastore options
while [ ! $# -eq 0 ]
do
  case "$1" in
    --store-on-disk)
      options="$options --store-on-disk"
      shift
      ;;
    --no-store-on-disk)
      options="$options --no-store-on-disk"
      shift
      ;;
    --consistency=*)
      consistency=${1#*=}
      options="$options --consistency=$consistency"
      shift
      ;;
    *)
      echo "Invalid option: $1. Use: --store-on-disk, --no-store-on-disk, --consistency=[0.0-1.0]"
      exit 1
      ;;
  esac
done

# Config gcloud project
gcloud config set project ${DATASTORE_PROJECT_ID}

# Start emulator
gcloud beta emulators datastore start \
  --data-dir=/opt/data \
  --host-port=${DATASTORE_LISTEN_ADDRESS} \
  ${options}
