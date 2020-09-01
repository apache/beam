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

"""Enumeration of URNs specific to the Python SDK.

For internal use only; no backwards-compatibility guarantees."""

PICKLED_CODER = "beam:coder:pickled_python:v1"
PICKLED_COMBINE_FN = "beam:combinefn:pickled_python:v1"
PICKLED_DOFN = "beam:dofn:pickled_python:v1"
PICKLED_DOFN_INFO = "beam:dofn:pickled_python_info:v1"
PICKLED_SOURCE = "beam:source:pickled_python:v1"
PICKLED_TRANSFORM = "beam:transform:pickled_python:v1"
PICKLED_WINDOW_MAPPING_FN = "beam:window_mapping_fn:pickled_python:v1"
PICKLED_WINDOWFN = "beam:window_fn:pickled_python:v1"
PICKLED_VIEWFN = "beam:view_fn:pickled_python_data:v1"

IMPULSE_READ_TRANSFORM = "beam:transform:read_from_impulse_python:v1"

GENERIC_COMPOSITE_TRANSFORM = "beam:transform:generic_composite:v1"

KEY_WITH_NONE_DOFN = "beam:dofn:python_key_with_none:v1"
PACKED_COMBINE_FN = "beam:combinefn:packed_python:v1"

# A coder for a tuple.
# Components: The coders for the tuple elements, in order.
TUPLE_CODER = "beam:coder:tuple:v1"

# Invoke UserFns in process, via direct function calls.
# Payload: None.
EMBEDDED_PYTHON = "beam:env:embedded_python:v1"

# Invoke UserFns in process, but over GRPC channels.
# Payload: (optional) Number of worker threads, followed by ',' and the size of
# the state cache, as a decimal string, e.g. '2,1000'.
EMBEDDED_PYTHON_GRPC = "beam:env:embedded_python_grpc:v1"

# Instantiate SDK harness via a command line provided in the payload.
# This is different than the standard process environment in that it
# starts up the SDK harness directly, rather than the bootstrapping
# and artifact fetching code.
# (Used for testing.)
SUBPROCESS_SDK = "beam:env:harness_subprocess_python:v1"
