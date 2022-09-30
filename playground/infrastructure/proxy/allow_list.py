# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ALLOWED_LIST contains all hosts that are allowed to make requests
# from Beam Playground
ALLOWED_LIST = [
    "localhost",
    "127.0.0.1",
    "logging.googleapis.com",
    "datastore.googleapis.com",
    "oauth2.googleapis.com"
]

# ALLOWED_BUCKET_LIST contains all public Google Cloud Storage buckets
# that are allowed to make requests from Beam Playground
ALLOWED_BUCKET_LIST = [
    "dataflow-samples",
    "beam-samples",
    "apache-beam-samples",
]
