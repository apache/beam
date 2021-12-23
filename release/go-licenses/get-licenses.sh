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

set -ex
rm -rf /output/*

# TODO(BEAM-13538): Replace w/current RC tag (eg v2.34.0_RC01 or similar) passed in from gradle.
# Or that tag's commit for reproducible go-licenses.
VERSION=master

# Make a dummy dir for the go mod file project.
mkdir dummy; cd dummy
go mod init dummy.com/release

# We just need to download the package, not build it.
go get -d $sdk_location@$VERSION

go-licenses save $sdk_location --save_path=/output/licenses
go-licenses csv $sdk_location | tee /output/licenses/list.csv
chmod -R a+w /output/*
