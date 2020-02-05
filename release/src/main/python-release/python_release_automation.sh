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

# Arguments:
#   $1 - python interpreter version: [python2.7, python3.5, ...]
#   $2 - runner type: [direct, dataflow]
#   $3 - test type: [quick_start, mobile_game]

if [[ "$3" == "quick_start" ]]; then
  run_release_candidate_python_quickstart "tar"   "python${1}" "${2}"
  run_release_candidate_python_quickstart "wheel" "python${1}" "${2}"
elif [[ "$3" == "mobile_game" ]]; then
  run_release_candidate_python_mobile_gaming "tar"   "python${1}" "${2}"
  run_release_candidate_python_mobile_gaming "wheel" "python${1}" "${2}"
fi
