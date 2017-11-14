#!/bin/bash
#
# Copyright 2017 Seznam.cz, a.s.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


FLINK=${FLINK:-/tmp/flink-1.2.1/bin/flink}

JAR=${1}; shift
CLASS=${1}; shift
CONF=${1}; shift

if [ -z "$CONF" -o -z "$JAR" -o -z "$CLASS" ] ; then
	echo "Usage: $0 <jar-file> fully-qualified-class-name <config-file> [params]"
	exit 1
fi

sudo HADOOP_HOME=$HADOOP_HOME -u fulltext \
    "${FLINK}" run \
    -m yarn-cluster \
    -yn 30 \
    -ys 4 \
    -ytm 5300 \
    -yD taskmanager.network.numberOfBuffers=6144 \
    -yD containerized.heap-cutoff-ratio=0.35 \
    -c "$CLASS" \
    "$JAR" \
    "$CONF" \
    $*
