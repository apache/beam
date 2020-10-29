#
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
#

"""Various names shared by more than one runner."""

# All constants are for internal use only; no backwards-compatibility
# guarantees.

# Current value is hardcoded in Dataflow internal infrastructure;
# please don't change without a review from Dataflow maintainers.
STAGED_SDK_SOURCES_FILENAME = 'dataflow_python_sdk.tar'

PICKLED_MAIN_SESSION_FILE = 'pickled_main_session'
STAGED_PIPELINE_FILENAME = "pipeline.pb"
STAGED_PIPELINE_URL_METADATA_FIELD = 'pipeline_url'

# Package names for different distributions
BEAM_PACKAGE_NAME = 'apache-beam'

# SDK identifiers for different distributions
BEAM_SDK_NAME = 'Apache Beam SDK for Python'
