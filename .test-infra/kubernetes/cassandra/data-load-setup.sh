# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash
set -e

# Load YCSB tool
echo "Downloading YCSB tool"
echo "------------------------------"
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.12.0/ycsb-0.12.0.tar.gz
tar xfz ycsb-0.12.0.tar.gz
wget https://www.slf4j.org/dist/slf4j-1.7.22.tar.gz
tar xfz slf4j-1.7.22.tar.gz
cp slf4j-1.7.22/slf4j-simple-*.jar ycsb-0.12.0/lib/
cp slf4j-1.7.22/slf4j-api-*.jar ycsb-0.12.0/lib/
echo "YCSB tool loaded"
