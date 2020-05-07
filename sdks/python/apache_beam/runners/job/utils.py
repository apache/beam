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

"""Utility functions for efficiently processing with the job API
"""

# pytype: skip-file

from __future__ import absolute_import

import json

from google.protobuf import json_format
from google.protobuf import struct_pb2


def dict_to_struct(dict_obj):
  # type: (dict) -> struct_pb2.Struct
  return json_format.ParseDict(dict_obj, struct_pb2.Struct())


def struct_to_dict(struct_obj):
  # type: (struct_pb2.Struct) -> dict
  return json.loads(json_format.MessageToJson(struct_obj))
