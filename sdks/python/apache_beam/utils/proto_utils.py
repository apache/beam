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

"""For internal use only; no backwards-compatibility guarantees."""

from __future__ import absolute_import

from google.protobuf import any_pb2
from google.protobuf import struct_pb2


def pack_Any(msg):
  """Creates a protobuf Any with msg as its content.

  Returns None if msg is None.
  """
  if msg is None:
    return None

  result = any_pb2.Any()
  result.Pack(msg)
  return result


def unpack_Any(any_msg, msg_class):
  """Unpacks any_msg into msg_class.

  Returns None if msg_class is None.
  """
  if msg_class is None:
    return None
  msg = msg_class()
  any_msg.Unpack(msg)
  return msg


def parse_Bytes(serialized_bytes, msg_class):
  """Parses the String of bytes into msg_class.

  Returns the input bytes if msg_class is None."""
  if msg_class is None or msg_class is bytes:
    return serialized_bytes
  msg = msg_class()
  msg.ParseFromString(serialized_bytes)
  return msg


def pack_Struct(**kwargs):
  """Returns a struct containing the values indicated by kwargs.
  """
  msg = struct_pb2.Struct()
  for key, value in kwargs.items():
    msg[key] = value  # pylint: disable=unsubscriptable-object, unsupported-assignment-operation
  return msg


def from_micros(cls, micros):
  result = cls()
  result.FromMicroseconds(micros)
  return result
