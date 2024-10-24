#!/bin/bash -uex
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
#
# Usage: ./setup_sdk.sh /opt/playground/prepared_folder
# Setup Go Beam SDK dev environment, so that examples' dependencies were met.
# Consumed envs:
# - PREPARED_MOD_DIR: result module path
# - SDK_TAG: desired SDK version, for ex. "2.42.0", "2.44.0dev"
# - SDK_TAG_LOCAL: special value for SDK_TAG for the local Go SDK (defined by build system)
# - BEAM_SRC: Go SDK location

PIPELINES_MODULE="executable_files"

# Project convention for local versions 2.44.0.dev is incorrect in Go
# Replace this with 2.44.0-dev
SDK_TAG=${SDK_TAG/%.dev/-dev}
SDK_TAG_LOCAL=${SDK_TAG_LOCAL/%.dev/-dev}

mkdir -p $PREPARED_MOD_DIR
cd $PREPARED_MOD_DIR

BEAM_PKG=github.com/apache/beam/sdks/v2

go mod init $PIPELINES_MODULE
go mod edit -require=$BEAM_PKG@v$SDK_TAG

if [ "$SDK_TAG" == "$SDK_TAG_LOCAL" ]; then
  go mod edit -replace=$BEAM_PKG@v$SDK_TAG=$BEAM_SRC
fi

go install -x $BEAM_PKG/go/pkg/beam@v$SDK_TAG
go install -x $BEAM_PKG/go/pkg/beam/io/bigqueryio@v$SDK_TAG
go install -x $BEAM_PKG/go/test/integration@v$SDK_TAG

go mod download -x all
