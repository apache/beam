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
import logging
import os
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external_transform_provider import STANDARD_URN_PATTERN
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
from apache_beam.transforms.external_transform_provider import camel_case_to_snake_case
from apache_beam.transforms.external_transform_provider import infer_name_from_identifier
from apache_beam.transforms.external_transform_provider import snake_case_to_lower_camel_case
from apache_beam.transforms.external_transform_provider import snake_case_to_upper_camel_case


class NameUtilsTest(unittest.TestCase):
  def test_snake_case_to_upper_camel_case(self):
    test_cases = [("", ""), ("test", "Test"), ("test_name", "TestName"),
                  ("test_double_underscore", "TestDoubleUnderscore"),
                  ("TEST_CAPITALIZED", "TestCapitalized"),
                  ("_prepended_underscore", "PrependedUnderscore"),
                  ("appended_underscore_", "AppendedUnderscore")]
    for case in test_cases:
      self.assertEqual(case[1], snake_case_to_upper_camel_case(case[0]))

  def test_snake_case_to_lower_camel_case(self):
    test_cases = [("", ""), ("test", "test"), ("test_name", "testName"),
                  ("test_double_underscore", "testDoubleUnderscore"),
                  ("TEST_CAPITALIZED", "testCapitalized"),
                  ("_prepended_underscore", "prependedUnderscore"),
                  ("appended_underscore_", "appendedUnderscore")]
    for case in test_cases:
      self.assertEqual(case[1], snake_case_to_lower_camel_case(case[0]))

  def test_camel_case_to_snake_case(self):
    test_cases = [("", ""), ("Test", "test"), ("TestName", "test_name"),
                  ("TestDoubleUnderscore",
                   "test_double_underscore"), ("MyToLoFo", "my_to_lo_fo"),
                  ("BEGINNINGAllCaps",
                   "beginning_all_caps"), ("AllCapsENDING", "all_caps_ending"),
                  ("AllCapsMIDDLEWord", "all_caps_middle_word"),
                  ("lowerCamelCase", "lower_camel_case")]
    for case in test_cases:
      self.assertEqual(case[1], camel_case_to_snake_case(case[0]))

  def test_infer_name_from_identifier(self):
    standard_test_cases = [
        ("beam:schematransform:org.apache.beam:transform:v1", "Transform"),
        ("beam:schematransform:org.apache.beam:my_transform:v1",
         "MyTransform"), (
             "beam:schematransform:org.apache.beam:my_transform:v2",
             "MyTransformV2"),
        ("beam:schematransform:org.apache.beam:fe_fi_fo_fum:v2", "FeFiFoFumV2"),
        ("beam:schematransform:bad_match:my_transform:v1", None)
    ]
    for case in standard_test_cases:
      self.assertEqual(
          case[1], infer_name_from_identifier(case[0], STANDARD_URN_PATTERN))

    custom_pattern_cases = [
        # (<pattern>, <urn>, <expected output>)
        (
            r"^custom:transform:([\w-]+):(\w+)$",
            "custom:transform:my_transform:v1",
            "MyTransformV1"),
        (
            r"^org.user:([\w-]+):([\w-]+):([\w-]+):external$",
            "org.user:some:custom_transform:we_made:external",
            "SomeCustomTransformWeMade"),
        (
            r"^([\w-]+):user.transforms",
            "my_eXTErnal:user.transforms",
            "MyExternal"),
        (r"^([\w-]+):user.transforms", "my_external:badinput.transforms", None),
    ]
    for case in custom_pattern_cases:
      self.assertEqual(case[2], infer_name_from_identifier(case[1], case[0]))


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_PORT'),
    "EXPANSION_PORT environment var is not provided.")
class ExternalTransformProviderTest(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

  def test_generate_sequence_config_schema_and_description(self):
    provider = ExternalTransformProvider(
        BeamJarExpansionService(":sdks:java:io:expansion-service:shadowJar"))

    self.assertTrue((
        'GenerateSequence',
        'beam:schematransform:org.apache.beam:generate_sequence:v1'
    ) in provider.get_available())

    GenerateSequence = provider.get('GenerateSequence')
    config_schema = GenerateSequence.configuration_schema
    for param in ['start', 'end', 'rate']:
      self.assertTrue(param in config_schema)

    description_substring = (
        "Outputs a PCollection of Beam Rows, each "
        "containing a single INT64")
    self.assertTrue(description_substring in GenerateSequence.description)

  def test_run_generate_sequence(self):
    provider = ExternalTransformProvider(
        BeamJarExpansionService(":sdks:java:io:expansion-service:shadowJar"))

    with beam.Pipeline() as p:
      numbers = p | provider.GenerateSequence(
          start=0, end=10) | beam.Map(lambda row: row.value)

      assert_that(numbers, equal_to([i for i in range(10)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
