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

"""Various names for properties, transforms, etc."""

# All constants are for internal use only; no backwards-compatibility
# guarantees.

# pytype: skip-file

# Referenced by Dataflow legacy worker.
from apache_beam.runners.internal.names import PICKLED_MAIN_SESSION_FILE  # pylint: disable=unused-import

# String constants related to sources framework
SOURCE_FORMAT = 'custom_source'
SOURCE_TYPE = 'CustomSourcesType'
SERIALIZED_SOURCE_KEY = 'serialized_source'

# In a released SDK, Python sdk container image is tagged with the SDK version.
# Unreleased sdks use container image tag specified below.
# Update this tag whenever there is a change that
# requires changes to SDK harness container or SDK harness launcher.
BEAM_DEV_SDK_CONTAINER_TAG = 'beam-master-20240918'

DATAFLOW_CONTAINER_IMAGE_REPOSITORY = 'gcr.io/cloud-dataflow/v1beta3'
