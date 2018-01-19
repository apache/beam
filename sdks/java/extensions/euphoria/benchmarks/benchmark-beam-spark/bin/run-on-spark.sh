#!/bin/bash
#
# Copyright 2016-2018 Seznam.cz, a.s.
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


SPARK_SUBMIT=${SPARK_SUBMIT:-/www/spark/1.6.3/bin/spark-submit}
JAR=${1}; shift
CLASS=${1}; shift
CONF=${1}; shift

if [ -z "$CONF" -o -z "$JAR" -o -z "$CLASS" ] ; then
	echo "Usage: $0 <jar-file> fully-qualified-class-name <config-file> [params]"
	exit 1
fi

"${SPARK_SUBMIT}" \
    --deploy-mode cluster --master yarn \
    --executor-memory 8G \
    --driver-memory 2G \
    --num-executors=30 \
    --executor-cores=4 \
    --conf spark.yarn.executor.memoryOverhead=2000 \
    --conf spark.yarn.driver.memoryOverhead=1000  \
    --files "${CONF}" \
    --class "${CLASS}" \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.kryo.registrationRequired=true \
    --conf spark.memory.fraction=0.5 \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.kryo.unsafe=false \
    "${JAR}" \
    `basename $CONF` \
    spark \
    --usesProvidedSparkContext=true \
    "$*"
