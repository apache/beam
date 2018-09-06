#!/bin/bash -e
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

# Updates the examples archetype to match the examples module
#
# Usage: Invoke with no arguments from any working directory.

# The directory of this script. Assumes root of the maven-archetypes module.
HERE="$( dirname "$0" )"

# The directory of the examples-java module
EXAMPLES_ROOT="${HERE}/../../../../examples/java"

# The root of the examples archetype
ARCHETYPE_ROOT="${HERE}/src/main/resources/archetype-resources"

mkdir -p "${ARCHETYPE_ROOT}/src/main/java"
mkdir -p "${ARCHETYPE_ROOT}/src/test/java"

#
# Copy the Java subset of the examples project verbatim. 
#
rsync -a --exclude cookbook --exclude complete --exclude snippets  \
    "${EXAMPLES_ROOT}"/src/main/java/org/apache/beam/examples/     \
    "${ARCHETYPE_ROOT}/src/main/java"                              \
    --delete

rsync -a --exclude cookbook --exclude complete --exclude snippets --exclude '*IT.java'  \
    "${EXAMPLES_ROOT}"/src/test/java/org/apache/beam/examples/                          \
    "${ARCHETYPE_ROOT}/src/test/java"                                                   \
    --delete

mkdir -p "${ARCHETYPE_ROOT}/src/main/java/complete/game"
mkdir -p "${ARCHETYPE_ROOT}/src/test/java/complete/game"

#
# Copy the Java mobile gaming examples.
#
rsync -a                                                                      \
    "${EXAMPLES_ROOT}"/src/main/java/org/apache/beam/examples/complete/game/  \
    "${ARCHETYPE_ROOT}/src/main/java/complete/game"

rsync -a                                                                      \
    "${EXAMPLES_ROOT}"/src/test/java/org/apache/beam/examples/complete/game/  \
    "${ARCHETYPE_ROOT}/src/test/java/complete/game"

#
# Replace 'package org.apache.beam.examples' with 'package ${package}' in all Java code
#
find "${ARCHETYPE_ROOT}/src/main/java" -name '*.java' -print0 \
    | xargs -0 sed -i.bak 's/^package org\.apache\.beam\.examples/package ${package}/g'

find "${ARCHETYPE_ROOT}/src/test/java" -name '*.java' -print0 \
    | xargs -0 sed -i.bak 's/^package org\.apache\.beam\.examples/package ${package}/g'

#
# Replace 'import org.apache.beam.examples.' with 'import ${package}.' in all Java code
#
find "${ARCHETYPE_ROOT}/src/main/java" -name '*.java' -print0 \
    | xargs -0 sed -i.bak 's/^import org\.apache\.beam\.examples/import ${package}/g'

find "${ARCHETYPE_ROOT}/src/test/java" -name '*.java' -print0 \
    | xargs -0 sed -i.bak 's/^import org\.apache\.beam\.examples/import ${package}/g'

#
# The use of -i.bak is necessary for the above to work with both GNU and BSD sed. 
# Delete the files now.
#
find "${ARCHETYPE_ROOT}/src" -name '*.bak' -delete
