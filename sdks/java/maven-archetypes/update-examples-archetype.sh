#!/bin/bash -ex

HERE="$(dirname $0)"

EXAMPLES_ROOT="${HERE}/../../../examples/java"
ARCHETYPE_ROOT="${HERE}/examples/src/main/resources/archetype-resources"

#
# Copy the Java subset of the examples project verbatim. 
#
rsync -a --exclude cookbook --exclude complete                  \
    "${EXAMPLES_ROOT}"/src/main/java/org/apache/beam/examples/  \
    "${ARCHETYPE_ROOT}/src/main/java"

rsync -a --exclude cookbook --exclude complete --exclude '*IT.java'  \
    "${EXAMPLES_ROOT}"/src/test/java/org/apache/beam/examples/        \
    "${ARCHETYPE_ROOT}/src/test/java"

#
# Replace 'package org.apache.beam.examples' with 'package ${package}' in all Java code
#
find "${ARCHETYPE_ROOT}/src/main/java" -name '*.java' -print0 \
    | xargs -0 sed -i 's/^package org\.apache\.beam\.examples/package ${package}/g'

find "${ARCHETYPE_ROOT}/src/test/java" -name '*.java' -print0 \
    | xargs -0 sed -i 's/^package org\.apache\.beam\.examples/package ${package}/g'

#
# Replace 'import org.apache.beam.examples.' with 'import ${package}.' in all Java code
#
find "${ARCHETYPE_ROOT}/src/main/java" -name '*.java' -print0 \
    | xargs -0 sed -i 's/^import org\.apache\.beam\.examples/import ${package}/g'

find "${ARCHETYPE_ROOT}/src/test/java" -name '*.java' -print0 \
    | xargs -0 sed -i 's/^import org\.apache\.beam\.examples/import ${package}/g'
