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

"""Coder for AvroRecord serialization/deserialization."""

from __future__ import absolute_import

import json
from io import BytesIO

from fastavro import parse_schema
from fastavro import schemaless_reader
from fastavro import schemaless_writer

from apache_beam.coders.coder_impl import SimpleCoderImpl
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import FastCoder

AVRO_CODER_URN = "beam:coder:avro:v1"

__all__ = ['AvroCoder', 'AvroRecord']


class AvroCoder(FastCoder):
  """A coder used for AvroRecord values."""

  def __init__(self, schema):
    self.schema = schema

  def _create_impl(self):
    return AvroCoderImpl(self.schema)

  def is_deterministic(self):
    # TODO: need to confirm if it's deterministic
    return False

  def __eq__(self, other):
    return (type(self) == type(other)
            and self.schema == other.schema)

  def __hash__(self):
    return hash(self.schema)

  def to_type_hint(self):
    return AvroRecord

  def to_runner_api_parameter(self, context):
    return AVRO_CODER_URN, self.schema, ()

  @Coder.register_urn(AVRO_CODER_URN, bytes)
  def from_runner_api_parameter(payload, unused_components, unused_context):
    return AvroCoder(payload)


class AvroCoderImpl(SimpleCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""

  def __init__(self, schema):
    self.parsed_schema = parse_schema(json.loads(schema))

  def encode(self, value):
    assert issubclass(type(value), AvroRecord)
    with BytesIO() as buf:
      schemaless_writer(buf, self.parsed_schema, value.record)
      return buf.getvalue()

  def decode(self, encoded):
    with BytesIO(encoded) as buf:
      return AvroRecord(schemaless_reader(buf, self.parsed_schema))


class AvroRecord(object):
  """Simple wrapper class for dictionary records."""

  def __init__(self, value):
    self.record = value

  def __eq__(self, other):
    return (
        issubclass(type(other), AvroRecord) and
        self.record == other.record
    )

  def __hash__(self):
    return hash(self.record)
