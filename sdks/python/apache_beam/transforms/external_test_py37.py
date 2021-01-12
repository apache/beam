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

"""Unit tests for the transform.external classes."""

# pytype: skip-file

from __future__ import absolute_import

import dataclasses
import typing
import unittest

import apache_beam as beam
from apache_beam import typehints
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms.external_test import PayloadBase


def get_payload(cls):
  payload = ExternalConfigurationPayload()
  payload.ParseFromString(cls._payload)
  return payload


class ExternalDataclassesPayloadTest(PayloadBase, unittest.TestCase):
  def get_payload_from_typing_hints(self, values):
    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      boolean: bool
      string_example: str
      list_of_strings: typing.List[str]
      mapping: typing.Mapping[str, float] = dataclasses.field(default=dict)
      optional_integer: typing.Optional[int] = None
      expansion_service: dataclasses.InitVar[typing.Optional[str]] = None

    return get_payload(DataclassTransform(**values))

  def get_payload_from_beam_typehints(self, values):
    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      boolean: bool
      string_example: str
      list_of_strings: typehints.List[str]
      mapping: typehints.Dict[str, float] = {}
      optional_integer: typehints.Optional[int] = None
      expansion_service: dataclasses.InitVar[typehints.Optional[str]] = None

    return get_payload(DataclassTransform(**values))


if __name__ == '__main__':
  unittest.main()
