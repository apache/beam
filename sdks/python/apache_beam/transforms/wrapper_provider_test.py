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

import os
import pytest
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.wrapper_provider import WrapperProvider


@pytest.mark.uses_core_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_PORT'),
    "EXPANSION_PORT environment var is not provided.")
class WrapperProviderTest(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

  def test_generate_sequence_config_schema(self):
    wrapper_provider = WrapperProvider(
        BeamJarExpansionService(":sdks:java:core:expansion-service:shadowJar"))

    self.assertTrue('GenerateSequence' in wrapper_provider.get_available())
    generate_sequence = wrapper_provider.get('GenerateSequence')

    config_schema = generate_sequence.configuration_schema
    for param in ['start', 'end', 'rate']:
      self.assertTrue(param in config_schema)

  def test_run_generate_sequence(self):
    wrapper_provider = WrapperProvider(
        BeamJarExpansionService(":sdks:java:core:expansion-service:shadowJar"))

    self.assertTrue('GenerateSequence' in wrapper_provider.get_available())

    generate_sequence = wrapper_provider.get('GenerateSequence')

    with beam.Pipeline() as p:
      numbers = p | generate_sequence(
          start=0, end=10) | beam.Map(lambda row: row.value)

      assert_that(numbers, equal_to([i for i in range(10)]))
