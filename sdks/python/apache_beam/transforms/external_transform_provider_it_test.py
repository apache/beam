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
import importlib
import inspect
import logging
import os
import secrets
import shutil
import time
import typing
import unittest
from os.path import dirname

import numpy
import pytest
import yaml

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external_transform_provider import STANDARD_URN_PATTERN
from apache_beam.transforms.external_transform_provider import ExternalTransform
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
from apache_beam.transforms.external_transform_provider import infer_name_from_identifier
from apache_beam.transforms.external_transform_provider import snake_case_to_upper_camel_case
from apache_beam.transforms.xlang.io import GenerateSequence


class NameAndTypeUtilsTest(unittest.TestCase):
  def test_snake_case_to_upper_camel_case(self):
    test_cases = [("", ""), ("test", "Test"), ("test_name", "TestName"),
                  ("test_double_underscore", "TestDoubleUnderscore"),
                  ("TEST_CAPITALIZED", "TestCapitalized"),
                  ("_prepended_underscore", "PrependedUnderscore"),
                  ("appended_underscore_", "AppendedUnderscore")]
    for case in test_cases:
      self.assertEqual(case[1], snake_case_to_upper_camel_case(case[0]))

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
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class ExternalTransformProviderIT(unittest.TestCase):
  def test_generate_sequence_signature_and_doc(self):
    provider = ExternalTransformProvider(
        BeamJarExpansionService(":sdks:java:io:expansion-service:shadowJar"))

    self.assertTrue((
        'GenerateSequence',
        'beam:schematransform:org.apache.beam:generate_sequence:v1'
    ) in provider.get_available())

    GenerateSequence = provider.get('GenerateSequence')
    signature = inspect.signature(GenerateSequence)
    for param in ['start', 'end', 'rate']:
      self.assertTrue(param in signature.parameters.keys())

    doc_substring = (
        "Outputs a PCollection of Beam Rows, each "
        "containing a single INT64")
    self.assertTrue(doc_substring in inspect.getdoc(GenerateSequence))

  def test_run_generate_sequence(self):
    provider = ExternalTransformProvider(
        BeamJarExpansionService(":sdks:java:io:expansion-service:shadowJar"))

    with beam.Pipeline() as p:
      numbers = p | provider.GenerateSequence(
          start=0, end=10) | beam.Map(lambda row: row.value)

      assert_that(numbers, equal_to([i for i in range(10)]))


@pytest.mark.xlang_wrapper_generation
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class AutoGenerationScriptIT(unittest.TestCase):
  """
  This class tests the generation and regeneration operations in
  `gen_xlang_wrappers.py`.
  """

  # tests cases will use GenerateSequence
  GEN_SEQ_IDENTIFIER = \
    'beam:schematransform:org.apache.beam:generate_sequence:v1'

  def setUp(self):
    # import script from top-level sdks/python directory
    self.sdk_dir = os.path.abspath(dirname(dirname(dirname(__file__))))
    spec = importlib.util.spec_from_file_location(
        'gen_xlang_wrappers',
        os.path.join(self.sdk_dir, 'gen_xlang_wrappers.py'))
    self.script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(self.script)
    args = TestPipeline(is_integration_test=True).get_full_options_as_args()
    runner = PipelineOptions(args).get_all_options()['runner']
    if runner and "direct" not in runner.lower():
      self.skipTest(
          "It is sufficient to run this test in the DirectRunner "
          "test suite only.")

    self.test_dir_name = 'test_gen_script_%d_%s' % (
        int(time.time()), secrets.token_hex(3))
    self.test_dir = os.path.join(
        os.path.abspath(dirname(__file__)), self.test_dir_name)
    self.service_config_path = os.path.join(
        self.test_dir, "test_expansion_service_config.yaml")
    self.transform_config_path = os.path.join(
        self.test_dir, "test_transform_config.yaml")
    os.mkdir(self.test_dir)

  def tearDown(self):
    shutil.rmtree(self.test_dir, ignore_errors=False)

  def delete_and_validate(self):
    self.script.delete_generated_files(self.test_dir)
    self.assertEqual(len(os.listdir(self.test_dir)), 0)

  def test_script_fails_with_invalid_destinations(self):
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': 'apache_beam/some_nonexistent_dir'
        }
    }
    with self.assertRaises(ValueError):
      self.create_and_check_transforms_config_exists(expansion_service_config)

  def test_pretty_types(self):
    types = [
        typing.Optional[typing.List[str]],
        numpy.int16,
        str,
        typing.Dict[str, numpy.float64],
        typing.Optional[typing.Dict[str, typing.List[numpy.int64]]],
        typing.Dict[int, typing.Optional[str]]
    ]

    expected_type_names = [('List[str]', True), ('numpy.int16', False),
                           ('str', False), ('Dict[str, numpy.float64]', False),
                           ('Dict[str, List[numpy.int64]]', True),
                           ('Dict[int, Optional[str]]', False)]

    for i in range(len(types)):
      self.assertEqual(
          self.script.pretty_type(types[i]), expected_type_names[i])

  def create_and_check_transforms_config_exists(self, expansion_service_config):
    with open(self.service_config_path, 'w') as f:
      yaml.dump([expansion_service_config], f)

    self.script.generate_transforms_config(
        self.service_config_path, self.transform_config_path)
    self.assertTrue(os.path.exists(self.transform_config_path))

  def create_and_validate_transforms_config(
      self, expansion_service_config, expected_name, expected_destination):
    self.create_and_check_transforms_config_exists(expansion_service_config)

    with open(self.transform_config_path) as f:
      configs = yaml.safe_load(f)
      gen_seq_config = None
      for config in configs:
        if config['identifier'] == self.GEN_SEQ_IDENTIFIER:
          gen_seq_config = config
      self.assertIsNotNone(gen_seq_config)
      self.assertEqual(
          gen_seq_config['default_service'],
          expansion_service_config['gradle_target'])
      self.assertEqual(gen_seq_config['name'], expected_name)
      self.assertEqual(
          gen_seq_config['destinations']['python'], expected_destination)
      self.assertIn("end", gen_seq_config['fields'])
      self.assertIn("start", gen_seq_config['fields'])
      self.assertIn("rate", gen_seq_config['fields'])

  def get_module(self, dest):
    module_name = dest.replace('apache_beam/', '').replace('/', '_')
    module = 'apache_beam.transforms.%s.%s' % (self.test_dir_name, module_name)
    return importlib.import_module(module)

  def write_wrappers_to_destinations_and_validate(
      self, destinations: typing.List[str]):
    """
    Generate wrappers from the config path and validate all destinations are
    included.
    Then write wrappers to destinations and validate all destination paths
    exist.

    :return: Generated wrappers grouped by destination
    """
    grouped_wrappers = self.script.get_wrappers_from_transform_configs(
        self.transform_config_path)
    for dest in destinations:
      self.assertIn(dest, grouped_wrappers)

    # write to our test directory to avoid messing with other files
    self.script.write_wrappers_to_destinations(
        grouped_wrappers, self.test_dir, format_code=False)

    for dest in destinations:
      self.assertTrue(
          os.path.exists(
              os.path.join(
                  self.test_dir,
                  dest.replace('apache_beam/', '').replace('/', '_') + ".py")))
    return grouped_wrappers

  def test_script_workflow(self):
    expected_destination = 'apache_beam/transforms'
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': expected_destination
        }
    }

    self.create_and_validate_transforms_config(
        expansion_service_config, 'GenerateSequence', expected_destination)
    grouped_wrappers = self.write_wrappers_to_destinations_and_validate(
        [expected_destination])
    # at least the GenerateSequence wrapper is set to this destination
    self.assertGreaterEqual(len(grouped_wrappers[expected_destination]), 1)

    # check the wrapper exists in this destination and has correct properties
    output_module = self.get_module(expected_destination)
    self.assertTrue(hasattr(output_module, 'GenerateSequence'))
    # Since our config isn't skipping any transforms,
    # it should include these two Kafka IOs as well
    self.assertTrue(hasattr(output_module, 'KafkaWrite'))
    self.assertTrue(hasattr(output_module, 'KafkaRead'))
    self.assertTrue(
        isinstance(output_module.GenerateSequence(start=0), ExternalTransform))
    self.assertEqual(
        output_module.GenerateSequence.identifier, self.GEN_SEQ_IDENTIFIER)

    self.delete_and_validate()

  def test_script_workflow_with_modified_transforms(self):
    modified_name = 'ModifiedSequence'
    modified_dest = 'apache_beam/io/gcp'
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': 'apache_beam/transforms'
        },
        'transforms': {
            'beam:schematransform:org.apache.beam:generate_sequence:v1': {
                'name': modified_name,
                'destinations': {
                    'python': modified_dest
                }
            }
        }
    }

    self.create_and_validate_transforms_config(
        expansion_service_config, modified_name, modified_dest)

    grouped_wrappers = self.write_wrappers_to_destinations_and_validate(
        [modified_dest])
    self.assertIn(modified_name, grouped_wrappers[modified_dest][0])
    self.assertEqual(len(grouped_wrappers[modified_dest]), 1)

    # check the modified wrapper exists in the modified destination
    # and check it has the correct properties
    output_module = self.get_module(modified_dest)
    self.assertTrue(
        isinstance(output_module.ModifiedSequence(start=0), ExternalTransform))
    self.assertEqual(
        output_module.ModifiedSequence.identifier, self.GEN_SEQ_IDENTIFIER)

    self.delete_and_validate()

  def test_script_workflow_with_skipped_transform(self):
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
        },
        'skip_transforms': [
            'beam:schematransform:org.apache.beam:generate_sequence:v1'
        ]
    }

    with open(self.service_config_path, 'w') as f:
      yaml.dump([expansion_service_config], f)

    self.script.generate_transforms_config(
        self.service_config_path, self.transform_config_path)

    # gen sequence shouldn't exist in the transform config
    with open(self.transform_config_path) as f:
      transforms = yaml.safe_load(f)
      gen_seq_config = None
      for transform in transforms:
        if transform['identifier'] == self.GEN_SEQ_IDENTIFIER:
          gen_seq_config = transform
      self.assertIsNone(gen_seq_config)

  def test_run_pipeline_with_generated_transform(self):
    with beam.Pipeline() as p:
      numbers = (
          p | GenerateSequence(start=0, end=10)
          | beam.Map(lambda row: row.value))
      assert_that(numbers, equal_to([i for i in range(10)]))

  def test_check_standard_external_transforms_config_in_sync(self):
    """
    This test creates a transforms config file and checks it against
    `sdks/standard_external_transforms.yaml`. Fails if the two configs don't
     match.

    Fix by running `./gradlew generateExternalTransformsConfig` and
    committing the changes.
    """
    sdks_dir = os.path.abspath(dirname(self.sdk_dir))
    self.script.generate_transforms_config(
        os.path.join(sdks_dir, 'standard_expansion_services.yaml'),
        self.transform_config_path)
    with open(self.transform_config_path) as f:
      test_config = yaml.safe_load(f)
    with open(os.path.join(sdks_dir, 'standard_external_transforms.yaml'),
              'r') as f:
      standard_config = yaml.safe_load(f)

    self.assertEqual(
        test_config,
        standard_config,
        "The standard xlang transforms config file "
        "\"standard_external_transforms.yaml\" is out of sync! Please update"
        "by running './gradlew generateExternalTransformsConfig'"
        "and committing the changes.")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
