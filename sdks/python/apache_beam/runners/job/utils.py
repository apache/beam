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

import grpc
import json
import logging

from google.protobuf import json_format
from google.protobuf import struct_pb2


def dict_to_struct(dict_obj: dict) -> struct_pb2.Struct:
  try:
    return json_format.ParseDict(dict_obj, struct_pb2.Struct())
  except json_format.ParseError:
    logging.error('Failed to parse dict %s', dict_obj)
    raise


def struct_to_dict(struct_obj: struct_pb2.Struct) -> dict:
  return json.loads(json_format.MessageToJson(struct_obj))


def is_grpc_stream_closure_error(e, allow_deadline_exceeded=False):
  """Check whether a gRPC exception represents an expected stream termination
  by the runner during job shutdown, cancellation, or timeout.
  """
  if not isinstance(e, grpc.RpcError) or not hasattr(e, 'code'):
    return False
  expected_codes = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.CANCELLED}
  if allow_deadline_exceeded:
    expected_codes.add(grpc.StatusCode.DEADLINE_EXCEEDED)
  return e.code() in expected_codes
