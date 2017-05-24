#!/bin/bash -ex
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

# Updates the examples-java8 archetype to have selection of our examples
# for use in walkthroughs, etc.
#
# Usage: Invoke with no arguments from any working directory.

# The directory of this script. Assumes root of the maven-archetypes module.
HERE="$( dirname "$0" )"

# The directory of the examples-java and examples-java8 modules
EXAMPLES_ROOT="${HERE}/../../../../examples/java"
JAVA8_EXAMPLES_ROOT="${HERE}/../../../../examples/java8"

# The root of the examples archetype
ARCHETYPE_ROOT="${HERE}/src/main/resources/archetype-resources"

mkdir -p "${ARCHETYPE_ROOT}/src/main/java"
mkdir -p "${ARCHETYPE_ROOT}/src/test/java"

#
# Copy the Java 7 subset of the examples project verbatim. 
#
rsync -a --exclude cookbook --exclude complete                  \
    "${EXAMPLES_ROOT}"/src/main/java/org/apache/beam/examples/  \
    "${ARCHETYPE_ROOT}/src/main/java"				            \
    --delete

rsync -a --exclude cookbook --exclude complete --exclude '*IT.java'  \
    "${EXAMPLES_ROOT}"/src/test/java/org/apache/beam/examples/       \
    "${ARCHETYPE_ROOT}/src/test/java"				                 \
    --delete

#
# Copy in MinimalWordCountJava8 and mobile gaming example
#
rsync -a                                                              \
    "${JAVA8_EXAMPLES_ROOT}"/src/main/java/org/apache/beam/examples/  \
    "${ARCHETYPE_ROOT}/src/main/java"

rsync -a                                                              \
    "${JAVA8_EXAMPLES_ROOT}"/src/test/java/org/apache/beam/examples/  \
    "${ARCHETYPE_ROOT}/src/test/java"


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
