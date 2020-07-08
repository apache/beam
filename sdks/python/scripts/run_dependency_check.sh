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
# This script will be run by Jenkins as a Python dependency test.

set -euv

mkdir -p $WORKSPACE/src/build/dependencyUpdates
rm -f $WORKSPACE/src/build/dependencyUpdates/python_dependency_report.txt

# List all outdated dependencies and write results in pythonDependencyReport
echo "The following dependencies have later release versions:" > $WORKSPACE/src/build/dependencyUpdates/python_dependency_report.txt
pip list --outdated | sed -n '1,2!p' | while IFS= read -r line
do
  echo $line | while IFS=' ' read dep curr_ver new_ver type
  do
    echo $line
    echo " - $dep [$curr_ver -> $new_ver]" >> $WORKSPACE/src/build/dependencyUpdates/python_dependency_report.txt
  done
done
