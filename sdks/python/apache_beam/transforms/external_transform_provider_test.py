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
import secrets
import shutil
import time
import typing
import unittest
from importlib import import_module

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
from apache_beam.transforms.external_transform_provider import camel_case_to_snake_case
from apache_beam.transforms.external_transform_provider import infer_name_from_identifier
from apache_beam.transforms.external_transform_provider import snake_case_to_lower_camel_case
from apache_beam.transforms.external_transform_provider import snake_case_to_upper_camel_case

try:
  from gen_xlang_wrappers import PYTHON_SUFFIX
  from gen_xlang_wrappers import delete_generated_files
  from gen_xlang_wrappers import generate_transforms_config
  from gen_xlang_wrappers import get_wrappers_from_transform_configs
  from gen_xlang_wrappers import run_script
  from gen_xlang_wrappers import write_wrappers_to_destinations
  from gen_protos import PYTHON_SDK_ROOT
except ImportError:
  run_script = None  # type: ignore[assignment]


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
class ExternalTransformProviderTest(unittest.TestCase):
  def setUp(self):
    self.assertTrue(
        os.environ.get('EXPANSION_PORT'), "Expansion service port not found!")

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


@pytest.mark.uses_multiple_java_expansion_services
@unittest.skipIf(
    run_script is None,
    "Need access to gen_xlang_wrappers.py to run these tests")
class AutoGenerationScriptTest(unittest.TestCase):
  """
  This class tests the generation and regeneration operations in
  `sdks/python/gen_xlang_wrappers.py`.
  """

  # tests cases will use GenerateSequence
  GEN_SEQ_IDENTIFIER = \
    'beam:schematransform:org.apache.beam:generate_sequence:v1'

  def setUp(self):
    args = TestPipeline(is_integration_test=True).get_full_options_as_args()
    runner = PipelineOptions(args).get_all_options()['runner']
    if runner and "direct" not in runner.lower():
      self.skipTest(
          "It is sufficient to run this test in the DirectRunner "
          "test suite only.")

    self.test_dir_name = 'test_gen_script_%d_%s' % (
        int(time.time()), secrets.token_hex(3))
    self.test_dir = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), self.test_dir_name)
    self.service_config_path = os.path.join(
        self.test_dir, "test_expansion_service_config.yaml")
    self.transform_config_path = os.path.join(
        self.test_dir, "test_transform_config.yaml")
    os.mkdir(self.test_dir)

    # self.assertTrue(
    #     os.environ.get('EXPANSION_PORTS'), "Expansion service port not found!")
    # logging.info("EXPANSION_PORTS: %s", os.environ.get('EXPANSION_PORTS'))

  def tearDown(self):
    shutil.rmtree(self.test_dir, ignore_errors=False)

  def create_and_check_transforms_config_exists(self, expansion_service_config):
    with open(self.service_config_path, 'w') as f:
      yaml.dump([expansion_service_config], f)

    # test that transform config YAML file is created
    generate_transforms_config(
      self.service_config_path, self.transform_config_path)
    self.assertTrue(os.path.exists(self.transform_config_path))

  def create_and_validate_transforms_config(self, expansion_service_config, expected_name, expected_destination):
    self.create_and_check_transforms_config_exists(expansion_service_config)

    # test that transform config is populated correctly
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

  def get_module(self, path):
    return import_module(path.replace('/', '.') + PYTHON_SUFFIX.rstrip('.py'))

  def write_wrappers_to_destinations_and_validate(self, destinations: typing.List[str]):
    """
    Generate wrappers from the config path and validate all destinations are
    included.
    Then write wrappers to destinations and validate all destination paths
    exist.

    :return: Generated wrappers grouped by destination
    """
    # test the GenerateSequence wrapper is set to the right destination
    grouped_wrappers = get_wrappers_from_transform_configs(
      self.transform_config_path)
    for dest in destinations:
      self.assertIn(dest, grouped_wrappers)

    write_wrappers_to_destinations(grouped_wrappers)
    for dest in destinations:
      self.assertTrue(
        os.path.exists(os.path.join(PYTHON_SDK_ROOT, *(dest + PYTHON_SUFFIX).split('/'))))
    return grouped_wrappers

  def delete_and_validate(self, destination):
    delete_generated_files(self.test_dir)
    self.assertFalse(
      os.path.exists(
        os.path.join(PYTHON_SDK_ROOT, *(destination + PYTHON_SUFFIX).split('/'))))

  def test_script_workflow(self):
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
        }
    }
    expected_destination = \
      f'apache_beam/transforms/{self.test_dir_name}/generate_sequence'

    self.create_and_validate_transforms_config(expansion_service_config, 'GenerateSequence', expected_destination)
    grouped_wrappers = self.write_wrappers_to_destinations_and_validate([expected_destination])
    # only the GenerateSequence wrapper is set to this destination
    self.assertEqual(len(grouped_wrappers[expected_destination]), 1)

    # check the wrapper exists in this destination and has correct properties
    generate_sequence_et = self.get_module(expected_destination)
    self.assertTrue(hasattr(generate_sequence_et, 'GenerateSequence'))
    self.assertTrue(
        isinstance(
            generate_sequence_et.GenerateSequence(start=0),
            ExternalTransform))
    self.assertEqual(
        generate_sequence_et.GenerateSequence.identifier,
        self.GEN_SEQ_IDENTIFIER)

    # test that we successfully delete the destination
    self.delete_and_validate(expected_destination)

  def test_script_workflow_with_modified_transforms(self):
    modified_name = 'ModifiedSequence'
    modified_dest = \
      f'apache_beam/transforms/{self.test_dir_name}/new_dir/modified_gen_seq'
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
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
    os.mkdir(os.path.join(self.test_dir, 'new_dir'))

    self.create_and_validate_transforms_config(expansion_service_config, modified_name, modified_dest)

    # test that the code for 'ModifiedSequence' is set to the right destination
    grouped_wrappers = self.write_wrappers_to_destinations_and_validate([modified_dest])
    self.assertIn(modified_name, grouped_wrappers[modified_dest][0])
    self.assertEqual(len(grouped_wrappers[modified_dest]), 1)

    # check the modified wrapper exists in the modified destination
    # and check it has the correct properties
    modified_gen_seq_et = self.get_module(modified_dest)
    self.assertTrue(
        isinstance(
            modified_gen_seq_et.ModifiedSequence(start=0),
            ExternalTransform))
    self.assertEqual(
        modified_gen_seq_et.ModifiedSequence.identifier,
        self.GEN_SEQ_IDENTIFIER)

    # test that we successfully delete the destination
    self.delete_and_validate(modified_dest)

  def test_script_workflow_with_multiple_wrappers_same_destination(self):
    modified_dest = f'apache_beam/transforms/{self.test_dir_name}/my_wrappers'
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
        },
        'transforms': {
            'beam:schematransform:org.apache.beam:generate_sequence:v1': {
                'destinations': {
                    'python': modified_dest
                }
            },
            'beam:schematransform:org.apache.beam:kafka_read:v1': {
                'destinations': {
                    'python': modified_dest
                }
            },
            'beam:schematransform:org.apache.beam:kafka_write:v1': {
                'destinations': {
                    'python': modified_dest
                }
            }
        }
    }
    self.create_and_check_transforms_config_exists(expansion_service_config)

    # test that our transform configs have the same destination
    with open(self.transform_config_path) as f:
      transforms = yaml.safe_load(f)
      for transform in transforms:
        if transform['identifier'] in expansion_service_config['transforms']:
          self.assertEqual(transform['destinations']['python'], modified_dest)

    # write wrappers to destination then check that all 3 exist there
    grouped_wrappers = self.write_wrappers_to_destinations_and_validate([modified_dest])
    self.assertEqual(len(grouped_wrappers[modified_dest]), 3)
    my_wrappers_et = self.get_module(modified_dest)
    self.assertTrue(hasattr(my_wrappers_et, 'GenerateSequence'))
    self.assertTrue(hasattr(my_wrappers_et, 'KafkaWrite'))
    self.assertTrue(hasattr(my_wrappers_et, 'KafkaRead'))
    self.delete_and_validate(modified_dest)

  def test_script_workflow_with_ignored_transform(self):
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
        },
        'ignore': ['beam:schematransform:org.apache.beam:generate_sequence:v1']
    }

    with open(self.service_config_path, 'w') as f:
      yaml.dump([expansion_service_config], f)

    generate_transforms_config(
        self.service_config_path, self.transform_config_path)

    # gen sequence shouldn't exist in the transform config
    with open(self.transform_config_path) as f:
      transforms = yaml.safe_load(f)
      gen_seq_config = None
      for transform in transforms:
        if transform['identifier'] == self.GEN_SEQ_IDENTIFIER:
          gen_seq_config = transform
      self.assertIsNone(gen_seq_config)

  def test_run_pipeline_with_script_generated_transform(self):
    modified_dest = f'apache_beam/transforms/{self.test_dir_name}/gen_seq'
    expansion_service_config = {
        "gradle_target": 'sdks:java:io:expansion-service:shadowJar',
        'destinations': {
            'python': f'apache_beam/transforms/{self.test_dir_name}'
        },
        'transforms': {
            'beam:schematransform:org.apache.beam:generate_sequence:v1': {
                'name': 'MyGenSeq', 'destinations': {
                    'python': modified_dest
                }
            }
        }
    }

    with open(self.service_config_path, 'w') as f:
      yaml.dump([expansion_service_config], f)
    generate_transforms_config(self.service_config_path, self.transform_config_path)
    wrappers_grouped_by_destination = get_wrappers_from_transform_configs(
        self.transform_config_path)
    write_wrappers_to_destinations(wrappers_grouped_by_destination)

    gen_seq_et = self.get_module(modified_dest)

    with beam.Pipeline() as p:
      numbers = (
          p | gen_seq_et.MyGenSeq(start=0, end=10)
          | beam.Map(lambda row: row.value))
      assert_that(numbers, equal_to([i for i in range(10)]))

  def test_check_standard_external_transforms_config_in_sync(self):
    """
    This test creates a transforms config file and checks it against the file
    in the SDK root `standard_external_transforms.yaml`. Fails if the
    test is out of sync.

    Fix by running `./gradlew generateExternalTransformWrappers` and
    committing the changes.
    """
    generate_transforms_config(
        os.path.join(PYTHON_SDK_ROOT, 'standard_expansion_services.yaml'),
        self.transform_config_path)
    with open(self.transform_config_path) as f:
      test_config = yaml.safe_load(f)
    with open(os.path.join(PYTHON_SDK_ROOT,
                           'standard_external_transforms.yaml'),
              'r') as f:
      standard_config = yaml.safe_load(f)

    self.assertEqual(
        test_config,
        standard_config,
        "The standard xlang transforms config file "
        "\"standard_external_transforms.yaml\" is out of sync! Please update"
        "by running './gradlew generateExternalTransformWrappers'"
        "and committing the changes.")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
