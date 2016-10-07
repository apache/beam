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

# This script generates Beam archetypes examples and starter projects,
# and verifies they can be built with 'mvn clean install'.
#
# This script should be executed after all other travis verifications,
# because it will generate artifacts and modifies pom files.

set -e

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[')

# mvn archetype:generate -pl sdks/java \
mvn archetype:generate -pl sdks/java \
  -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
  -DarchetypeGroupId=org.apache.beam \
  -DarchetypeVersion=$VERSION \
  -DgroupId=org.apache.beam.example \
  -DartifactId=test-beam-archetypes-examples \
  -Dversion="0.1" \
  -DinteractiveMode=false \
  -Dpackage=org.apache.beam.examples

mvn clean install -pl sdks/java/test-beam-archetypes-examples

mvn archetype:generate -pl sdks/java \
  -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-starter \
  -DarchetypeGroupId=org.apache.beam \
  -DarchetypeVersion=$VERSION \
  -DgroupId=org.apache.beam.starter \
  -DartifactId=test-beam-archetypes-starter \
  -Dversion="0.1" \
  -DinteractiveMode=false \
  -Dpackage=org.apache.beam.starter

mvn clean install -pl sdks/java/test-beam-archetypes-starter
