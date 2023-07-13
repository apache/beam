#!/bin/bash
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

# Check if APPLY_MIGRATIONS is set and apply mgirations in such case
if [ -n "$APPLY_MIGRATIONS" ]; then
    echo "Applying migrations"
    # If SDK_CONFIG is not set, set it to default value
    if [ -z "$SDK_CONFIG" ]; then
        SDK_CONFIG=/opt/playground/backend/sdks.yaml
    fi

    if [ -n "$DATASTORE_NAMESPACE" ]; then
        /opt/playground/backend/migration_tool -project-id $GOOGLE_CLOUD_PROJECT -sdk-config $SDK_CONFIG -namespace $DATASTORE_NAMESPACE
    else
        /opt/playground/backend/migration_tool -project-id $GOOGLE_CLOUD_PROJECT -sdk-config $SDK_CONFIG
    fi
fi

/opt/playground/backend/server_go_backend
