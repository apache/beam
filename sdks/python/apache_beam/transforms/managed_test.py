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

"""Unit tests for the managed transforms module."""

import unittest

import apache_beam as beam
from apache_beam.portability.common_urns import ManagedTransforms
from apache_beam.transforms.external import MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING
from apache_beam.transforms.managed import DELTA
from apache_beam.transforms.managed import DELTA_CDC
from apache_beam.transforms.managed import Read


class ManagedTest(unittest.TestCase):
  def test_delta_constants(self):
    self.assertEqual(DELTA, "delta")
    self.assertEqual(DELTA_CDC, "delta_cdc")
    self.assertIn("DELTA", beam.managed.__all__)
    self.assertIn("DELTA_CDC", beam.managed.__all__)

  def test_read_delta_transforms(self):
    read_delta = Read(DELTA, config={"table": "test_table"})
    self.assertEqual(read_delta._source, "delta")
    self.assertEqual(
        read_delta._underlying_identifier,
        ManagedTransforms.Urns.DELTA_LAKE_READ.urn)

    read_delta_cdc = Read(DELTA_CDC, config={"table": "test_table"})
    self.assertEqual(read_delta_cdc._source, "delta_cdc")
    self.assertEqual(
        read_delta_cdc._underlying_identifier,
        ManagedTransforms.Urns.DELTA_LAKE_CDC_READ.urn)

  def test_invalid_source_raises(self):
    with self.assertRaises(ValueError):
      Read("unsupported_source", config={})

  def test_expansion_service_resolution(self):
    self.assertIn(
        ManagedTransforms.Urns.DELTA_LAKE_READ.urn,
        MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING)
    self.assertIn(
        ManagedTransforms.Urns.DELTA_LAKE_CDC_READ.urn,
        MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING)


if __name__ == '__main__':
  unittest.main()
