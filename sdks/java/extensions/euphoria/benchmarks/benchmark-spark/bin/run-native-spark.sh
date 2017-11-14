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


SPARK_SUBMIT=${SPARK:-/tmp/spark-2.1.0/bin/spark-submit}

JAR=${1}; shift
CLASS=${1}; shift
CONF=${1}; shift

if [ -z "$CONF" -o -z "$JAR" -o -z "$CLASS" ] ; then
	echo "Usage: $0 <jar-file> fully-qualified-class-name <config-file> [params]"
	exit 1
fi

sudo -E -u fulltext \
	"${SPARK_SUBMIT}" \
	--deploy-mode cluster \
	--master yarn \
	--executor-memory 4G \
	--driver-memory 2G \
	--num-executors=30 \
	--executor-cores=4 \
	--conf spark.yarn.executor.memoryOverhead=3000 \
	--files "$CONF" \
	--class "$CLASS" \
	"$JAR" \
	`basename $CONF`
