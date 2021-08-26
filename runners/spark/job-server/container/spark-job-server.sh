#!/bin/sh
###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

### Just a simple script to bootstrap the SparkJobServerDriver
### For the environment, see the Dockerfile

# The following (forking to the background, then waiting) enables to use CTRL+C to kill the container.
# We're PID 1 which doesn't handle signals. By forking the Java process to the background,
# a PID > 1 is created which handles signals. After the command shuts down, the script and
# thus the container will also exit.

java -cp "jars/*" org.apache.beam.runners.spark.SparkJobServerDriver "$@" &
wait
