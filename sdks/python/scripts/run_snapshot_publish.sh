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

BUCKET=gs://beam-python-nightly-snapshots

VERSION=$(awk '/__version__/{print $3}' $WORKSPACE/sdks/python/apache_beam/version.py)
VERSION=$(echo $VERSION | cut -c 2- | rev | cut -c 2- | rev)
time=$(date +"%Y-%m-%dT%H:%M:%S")
SNAPSHOT="apache-beam-$VERSION-$time.tar.gz"

DEP_SNAPSHOT_ROOT="$BUCKET/dependency_requirements_snapshot"
DEP_SNAPSHOT_FILE_NAME="beam-py-requirements-$time.txt"

# Snapshots are built by Gradle task :sdks:python:depSnapshot
# and located under Gradle build directory.
cd $WORKSPACE/sdks/python/build

# Rename the file to be apache-beam-{VERSION}-{datetime}.tar.gz
# Notice that the distribution name of beam can be "apache-beam" with
# setuptools<69.3.0 or "apache_beam" with setuptools>=69.3.0.
for file in "apache[-_]beam-$VERSION*.tar.gz"; do
  mv $file $SNAPSHOT
done

# Upload to gcs bucket
gsutil cp $SNAPSHOT $BUCKET/$VERSION/

# Upload requirements.txt to gcs.
gsutil cp requirements.txt $DEP_SNAPSHOT_ROOT/$DEP_SNAPSHOT_FILE_NAME
