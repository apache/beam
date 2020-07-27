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

source release/src/main/python-release/run_release_candidate_python_quickstart.sh
source release/src/main/python-release/run_release_candidate_python_mobile_gaming.sh

for version in 2.7 3.5 3.6 3.7 3.8
do
  run_release_candidate_python_quickstart    "tar"   "python${version}"
  run_release_candidate_python_mobile_gaming "tar"   "python${version}"
  run_release_candidate_python_quickstart    "wheel" "python${version}"
  run_release_candidate_python_mobile_gaming "wheel" "python${version}"
done
