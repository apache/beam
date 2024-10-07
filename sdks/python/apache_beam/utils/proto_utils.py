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

# pytype: skip-file

from typing import Type
from typing import TypeVar
from typing import Union
from typing import overload

from google.protobuf import any_pb2
from google.protobuf import duration_pb2
from google.protobuf import message
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2

MessageT = TypeVar('MessageT', bound=message.Message)
TimeMessageT = TypeVar(
    'TimeMessageT', duration_pb2.Duration, timestamp_pb2.Timestamp)

message_types = (message.Message, )

_SECONDS_TO_MICROS = 10**6
_SECONDS_TO_NANOS = 10**9
_MICROS_TO_NANOS = 10**3


@overload
def pack_Any(msg: message.Message) -> any_pb2.Any:
  pass


@overload
def pack_Any(msg: None) -> None:
  pass


def pack_Any(msg):
  """Creates a protobuf Any with msg as its content.

  Returns None if msg is None.
  """
  if msg is None:
    return None

  result = any_pb2.Any()
  result.Pack(msg)
  return result


@overload
def unpack_Any(any_msg: any_pb2.Any, msg_class: Type[MessageT]) -> MessageT:
  pass


@overload
def unpack_Any(any_msg: any_pb2.Any, msg_class: None) -> None:
  pass


def unpack_Any(any_msg, msg_class):
  """Unpacks any_msg into msg_class.

  Returns None if msg_class is None.
  """
  if msg_class is None:
    return None
  msg = msg_class()
  any_msg.Unpack(msg)
  return msg


@overload
def parse_Bytes(serialized_bytes: bytes, msg_class: Type[MessageT]) -> MessageT:
  pass


@overload
def parse_Bytes(
    serialized_bytes: bytes, msg_class: Union[Type[bytes], None]) -> bytes:
  pass


def parse_Bytes(serialized_bytes, msg_class):
  """Parses the String of bytes into msg_class.

  Returns the input bytes if msg_class is None."""
  if msg_class is None or msg_class is bytes:
    return serialized_bytes
  msg = msg_class()
  msg.ParseFromString(serialized_bytes)
  return msg


def pack_Struct(**kwargs) -> struct_pb2.Struct:
  """Returns a struct containing the values indicated by kwargs.
  """
  msg = struct_pb2.Struct()
  for key, value in kwargs.items():
    msg[key] = value  # pylint: disable=unsubscriptable-object, unsupported-assignment-operation
  return msg


def from_micros(cls: Type[TimeMessageT], micros: int) -> TimeMessageT:
  result = cls()
  if isinstance(result, duration_pb2.Duration):
    result.FromMicroseconds(micros)
    return result
  # Protobuf 5.x enforces a maximum timestamp value less than the Beam
  # maximum allowable timestamp, so we cannot use the built-in conversion.
  elif isinstance(result, timestamp_pb2.Timestamp):
    result.seconds = micros // _SECONDS_TO_MICROS
    result.nanos = (micros % _SECONDS_TO_MICROS) * _MICROS_TO_NANOS
    return result
  else:
    raise RuntimeError('cannot convert the micro seconds to %s' % cls)


def to_micros(value: Union[duration_pb2.Duration, timestamp_pb2.Timestamp]):
  if isinstance(value, duration_pb2.Duration):
    return value.ToMicroseconds()
  # Protobuf 5.x enforces a maximum timestamp value less than the Beam
  # maximum allowable timestamp, so we cannot use the built-in conversion.
  elif isinstance(value, timestamp_pb2.Timestamp):
    micros = value.seconds * _SECONDS_TO_MICROS
    return micros + (value.nanos // _MICROS_TO_NANOS)
  else:
    raise RuntimeError('cannot convert %s to micro seconds' % value)


def to_Timestamp(time: Union[int, float]) -> timestamp_pb2.Timestamp:
  """Convert a float returned by time.time() to a Timestamp.
  """
  seconds = int(time)
  nanos = int((time - seconds) * 10**9)
  return timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)


def from_Timestamp(timestamp: timestamp_pb2.Timestamp) -> float:
  """Convert a Timestamp to a float expressed as seconds since the epoch.
  """
  return timestamp.seconds + float(timestamp.nanos) / 10**9
