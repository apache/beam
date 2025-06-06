# coding=utf-8
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
# pytype: skip-file
import glob
import logging
import os
import random
import sys
import unittest
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from unittest import mock

import pytest
import yaml

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.utils import subprocess_server
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
from apache_beam.yaml.readme_test import TestEnvironment
from apache_beam.yaml.readme_test import replace_recursive

from . import input_data


@beam.ptransform.ptransform_fn
def test_enrichment(
    pcoll,
    enrichment_handler: str,
    handler_config: Dict[str, Any],
    timeout: Optional[float] = 30):
  """
  Mocks the Enrichment transform for testing purposes.

  This PTransform simulates the behavior of the Enrichment transform by
  looking up data from predefined in-memory tables based on the provided
  `enrichment_handler` and `handler_config`.

  Note: The Github action that invokes these tests does not have gcp
  dependencies installed which is a prerequisite to
  apache_beam.transforms.enrichment.Enrichment as a top-level import.

  Args:
    pcoll: The input PCollection.
    enrichment_handler: A string indicating the type of enrichment handler
      to simulate (e.g., 'BigTable', 'BigQuery').
    handler_config: A dictionary containing configuration details for the
      simulated handler (e.g., table names, row keys, fields).
    timeout: An optional timeout value (ignored in this mock).

  Returns:
    A PCollection containing the enriched data.
  """

  if enrichment_handler == 'BigTable':
    row_key = handler_config['row_key']
    bt_data = INPUT_TABLES[(
        'BigTable', handler_config['instance_id'], handler_config['table_id'])]
    products = {str(data[row_key]): data for data in bt_data}

    def _fn(row):
      left = row._asdict()
      right = products[str(left[row_key])]
      left['product'] = left.get('product', None) or right
      return beam.Row(**left)
  elif enrichment_handler == 'BigQuery':
    row_key = handler_config['fields']
    dataset, table = handler_config['table_name'].split('.')[-2:]
    bq_data = INPUT_TABLES[('BigQuery', str(dataset), str(table))]
    bq_data = {
        tuple(str(data[key]) for key in row_key): data
        for data in bq_data
    }

    def _fn(row):
      left = row._asdict()
      right = bq_data[tuple(str(left[k]) for k in row_key)]
      row = {
          key: left.get(key, None) or right[key]
          for key in {*left.keys(), *right.keys()}
      }
      return beam.Row(**row)

  else:
    raise ValueError(f'{enrichment_handler} is not a valid enrichment_handler.')

  return pcoll | beam.Map(_fn)


@beam.ptransform.ptransform_fn
def test_kafka_read(
    pcoll,
    format,
    topic,
    bootstrap_servers,
    auto_offset_reset_config,
    consumer_config):
  return (
      pcoll | beam.Create(input_data.text_data().split('\n'))
      | beam.Map(lambda element: beam.Row(payload=element.encode('utf-8'))))


TEST_PROVIDERS = {
    'TestEnrichment': test_enrichment, 'TestReadFromKafka': test_kafka_read
}

INPUT_TRANSFORM_TEST_PROVIDERS = ['TestReadFromKafka']


def check_output(expected: List[str]):
  """
  Helper function to check the output of a pipeline against expected values.

  This function takes a list of expected output strings and returns a
  callable that can be used within a Beam pipeline to assert that the
  actual output matches the expected output.

  Args:
    expected: A list of strings representing the expected output elements.

  Returns:
    A callable that takes a list of PCollections and asserts their combined
    elements match the expected output.
  """
  def _check_inner(actual: List[PCollection[str]]):
    formatted_actual = actual | beam.Flatten() | beam.Map(
        lambda row: str(beam.Row(**row._asdict())))
    assert_matches_stdout(formatted_actual, expected)

  return _check_inner


def create_test_method(
    pipeline_spec_file: str,
    custom_preprocessors: List[Callable[..., Union[Dict, List]]]):
  """
  Generates a test method for a given YAML pipeline specification file.

  This function reads the YAML file, extracts the expected output (if present),
  and creates a test function that uses `TestPipeline` to run the pipeline
  defined in the YAML file. It also applies any custom preprocessors registered
  for this test.

  Args:
    pipeline_spec_file: The path to the YAML file containing the pipeline
      specification.
    custom_preprocessors: A list of preprocessor functions to apply before
      running the test.

  Returns:
    A test method (Callable) that can be added to a unittest.TestCase class.
  """
  @mock.patch('apache_beam.Pipeline', TestPipeline)
  def test_yaml_example(self):
    with open(pipeline_spec_file, encoding="utf-8") as f:
      lines = f.readlines()
    expected_key = '# Expected:\n'
    if expected_key in lines:
      expected = lines[lines.index('# Expected:\n') + 1:]
    else:
      raise ValueError(
          f"Missing '# Expected:' tag in example file '{pipeline_spec_file}'")
    for i, line in enumerate(expected):
      expected[i] = line.replace('#  ', '').replace('\n', '')
    pipeline_spec = yaml.load(
        ''.join(lines), Loader=yaml_transform.SafeLineLoader)

    with TestEnvironment() as env:
      for fn in custom_preprocessors:
        pipeline_spec = fn(pipeline_spec, expected, env)
      with beam.Pipeline(options=PipelineOptions(
          pickle_library='cloudpickle',
          **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
              'options', {})))) as p:
        actual = [
            yaml_transform.expand_pipeline(
                p,
                pipeline_spec,
                [
                    yaml_provider.InlineProvider(
                        TEST_PROVIDERS, INPUT_TRANSFORM_TEST_PROVIDERS)
                ])
        ]
        if not actual[0]:
          actual = list(p.transforms_stack[0].parts[-1].outputs.values())
          for transform in p.transforms_stack[0].parts[:-1]:
            if transform.transform.label == 'log_for_testing':
              actual += list(transform.outputs.values())
        check_output(expected)(actual)

  if 'deps' in pipeline_spec_file:
    test_yaml_example = pytest.mark.no_xdist(test_yaml_example)
    test_yaml_example = unittest.skipIf(
        sys.platform == 'win32', "Github virtualenv permissions issues.")(
            test_yaml_example)
    # This test fails, with an import error, for some (but not all) cloud
    # tox environments when run as a github action (not reproducible locally).
    # Adding debugging makes the failure go away.  All indications are that
    # this is some testing environmental issue.
    test_yaml_example = unittest.skipIf(
        '-cloud' in os.environ.get('TOX_ENV_NAME', ''),
        'Github actions environment issue.')(
            test_yaml_example)

  if 'java_deps' in pipeline_spec_file:
    test_yaml_example = pytest.mark.xlang_sql_expansion_service(
        test_yaml_example)
    test_yaml_example = unittest.skipIf(
        not os.path.exists(
            subprocess_server.JavaJarServer.path_to_dev_beam_jar(
                'sdks:java:extensions:sql:expansion-service:shadowJar')),
        "Requires expansion service jars.")(
            test_yaml_example)

  return test_yaml_example


class YamlExamplesTestSuite:
  """
  YamlExamplesTestSuites class is used to scan specified directories for .yaml
  files and dynamically generate a Python test method.  Additionally, it creates
  a method to complete some preprocessing for mocking IO.
  """
  _test_preprocessor: Dict[str, List[Callable[..., Union[Dict, List]]]] = {}

  def __init__(self, name: str, path: str):
    """
    Initializes the YamlExamplesTestSuite.

    Args:
      name: The name of the test suite. This will be used as the class name
        for the dynamically generated test suite.
      path: A string representing the path or glob pattern to search for
        YAML example files.
    """
    self._test_suite = self.create_test_suite(name, path)

  def run(self):
    """
    Runs the dynamically generated test suite.

    This method simply returns the test suite class created during
    initialization. The test runner (e.g., unittest.main()) can then be used
    to discover and run the tests within this suite.

    Returns:
      The dynamically created unittest.TestCase subclass.
    """
    return self._test_suite

  @classmethod
  def parse_test_methods(cls, path: str):
    """Scans a given path for YAML files and generates test methods.

    This method uses glob to find files matching the provided path. For each
    YAML file found, it constructs a unique test name and then calls
    `create_test_method` to generate the actual test function.
    It also retrieves any registered preprocessors for that specific test.

    Args:
      path: A string representing the path or glob pattern to search for
        YAML example files.

    Yields:
      A tuple containing the generated test name (str) and the
      corresponding test method (Callable).
    """
    files = glob.glob(path)
    if not files and os.path.exists(path) and os.path.isfile(path):
      files = [path]
    for file in files:
      test_name = f'test_{file.split(os.sep)[-1].replace(".", "_")}'
      custom_preprocessors = cls._test_preprocessor.get(test_name, [])
      yield test_name, create_test_method(file, custom_preprocessors)

  @classmethod
  def create_test_suite(cls, name: str, path: str):
    """Dynamically creates a unittest.TestCase subclass with generated tests.

    This method takes a suite name and a path (or glob pattern). It uses
    `parse_test_methods` to find YAML files at the given path and generate
    individual test methods for each. These generated test methods are then
    added as attributes to a new class, which is a subclass of
    `unittest.TestCase`.

    Args:
      name: The desired name for the dynamically created test suite class.
      path: A string representing the path or glob pattern to search for
        YAML example files, which will be used to generate test methods.

    Returns:
      A new class, subclass of `unittest.TestCase`, containing dynamically
      generated test methods based on the YAML files found at the given path.
    """
    return type(
        name, (unittest.TestCase, ), dict(cls.parse_test_methods(path)))

  @classmethod
  def register_test_preprocessor(cls, test_names: Union[str, List]):
    """Decorator to register a preprocessor function for specific tests.

    This decorator is used to associate a preprocessor function with one or
    more test names. The preprocessor function will be called before the
    corresponding test is executed, allowing for modification of the test
    specification or environment setup.

    Args:
      test_names: A string or a list of strings representing the names of the
        tests for which the preprocessor should be registered. The test names
        should match the names generated by `parse_test_methods`.

    Returns:
      A decorator function that takes the preprocessor function as an argument
      and registers it.
    """
    if isinstance(test_names, str):
      test_names = [test_names]

    def apply(preprocessor):
      for test_name in test_names:
        if test_name not in cls._test_preprocessor:
          cls._test_preprocessor[test_name] = []
        cls._test_preprocessor[test_name].append(preprocessor)
      return preprocessor

    return apply


@YamlExamplesTestSuite.register_test_preprocessor('test_wordcount_minimal_yaml')
def _wordcount_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for the wordcount_minimal.yaml test.

  This preprocessor generates a random input file based on the expected output
  of the wordcount example. This allows the test to verify the pipeline's
  correctness without relying on a fixed input file.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with the input file path replaced.
  """
  all_words = []
  for element in expected:
    word = element.split('=')[1].split(',')[0].replace("'", '')
    count = int(element.split('=')[2].replace(')', ''))
    all_words += [word] * count
  random.shuffle(all_words)

  lines = []
  while all_words:
    line_length = random.randint(1, min(10, len(all_words)))
    line = " ".join(
        all_words.pop(random.randrange(len(all_words)))
        for _ in range(line_length))
    lines.append(line)

  return replace_recursive(
      test_spec,
      'ReadFromText',
      'path',
      env.input_file('kinglear.txt', '\n'.join(lines)))


@YamlExamplesTestSuite.register_test_preprocessor('test_kafka_yaml')
def _kafka_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):

  test_spec = replace_recursive(
      test_spec,
      'ReadFromText',
      'path',
      env.input_file('kinglear.txt', input_data.text_data()))

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'ReadFromKafka':
        transform['type'] = 'TestReadFromKafka'

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_simple_filter_yaml',
    'test_simple_filter_and_combine_yaml',
    'test_iceberg_read_yaml',
    'test_iceberg_write_yaml',
    'test_kafka_yaml',
    'test_spanner_read_yaml',
    'test_spanner_write_yaml',
    'test_enrich_spanner_with_bigquery_yaml'
])
def _io_write_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve writing to IO.

  This preprocessor replaces any WriteTo transform with a LogForTesting
  transform. This allows the test to verify the data being written without
  actually writing to an external system.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with WriteTo transforms replaced.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '').startswith('WriteTo'):
        transform['type'] = 'LogForTesting'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if (k.startswith('__') or k == 'error_handling')
        }

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_simple_filter_yaml', 'test_simple_filter_and_combine_yaml'])
def _file_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  This preprocessor replaces any file IO ReadFrom transform with a Create
  transform that reads from a predefined in-memory dictionary. This allows
  the test to verify the pipeline's correctness without relying on external
  files.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with ReadFrom transforms replaced.
  """

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '').startswith('ReadFrom'):
        file_name = transform['config']['path'].split('/')[-1]
        return replace_recursive(
            test_spec,
            transform['type'],
            'path',
            env.input_file(file_name, INPUT_FILES[file_name]))

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(['test_iceberg_read_yaml'])
def _iceberg_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from Iceberg.

  This preprocessor replaces any ReadFromIceberg transform with a Create
  transform that reads from a predefined in-memory dictionary. This allows
  the test to verify the pipeline's correctness without relying on Iceberg
  tables stored externally.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with ReadFromIceberg transforms replaced.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'ReadFromIceberg':
        config = transform['config']
        (db_name, table_name,
         field_value_dynamic_destinations) = config['table'].split('.')

        transform['type'] = 'Create'
        transform['config'] = {
            k: v
            for k, v in config.items() if k.startswith('__')
        }
        transform['config']['elements'] = INPUT_TABLES[(
            str(db_name),
            str(table_name),
            str(field_value_dynamic_destinations))]

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_spanner_read_yaml', 'test_enrich_spanner_with_bigquery_yaml'])
def _spanner_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from Spanner.

  This preprocessor replaces any ReadFromSpanner transform with a Create
  transform that reads from a predefined in-memory dictionary. This allows
  the test to verify the pipeline's correctness without relying on external
  Spanner instances.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with ReadFromSpanner transforms replaced.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '').startswith('ReadFromSpanner'):
        config = transform['config']
        instance, database = config['instance_id'], config['database_id']
        if table := config.get('table', None) is None:
          table = config.get('query', '').split('FROM')[-1].strip()
        transform['type'] = 'Create'
        transform['config'] = {
            k: v
            for k, v in config.items() if k.startswith('__')
        }
        elements = INPUT_TABLES[(str(instance), str(database), str(table))]
        if config.get('query', None):
          config['query'].replace('select ',
                                  'SELECT ').replace(' from ', ' FROM ')
          columns = set(
              ''.join(config['query'].split('SELECT ')[1:]).split(
                  ' FROM', maxsplit=1)[0].split(', '))
          if columns != {'*'}:
            elements = [{
                column: element[column]
                for column in element if column in columns
            } for element in elements]
        transform['config']['elements'] = elements

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_bigtable_enrichment_yaml', 'test_enrich_spanner_with_bigquery_yaml'])
def _enrichment_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve the Enrichment transform.

  This preprocessor replaces the actual Enrichment transform with a mock
  `TestEnrichment` transform. This allows the test to verify the pipeline's
  correctness without requiring external services like BigTable or BigQuery.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with Enrichment transforms replaced.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '').startswith('Enrichment'):
        transform['type'] = 'TestEnrichment'

  return test_spec


INPUT_FILES = {'products.csv': input_data.products_csv()}
INPUT_TABLES = {
    ('shipment-test', 'shipment', 'shipments'): input_data.
    spanner_shipments_data(),
    ('orders-test', 'order-database', 'orders'): input_data.
    spanner_orders_data(),
    ('db', 'users', 'NY'): input_data.iceberg_dynamic_destinations_users_data(),
    ('BigTable', 'beam-test', 'bigtable-enrichment-test'): input_data.
    bigtable_data(),
    ('BigQuery', 'ALL_TEST', 'customers'): input_data.bigquery_data()
}
YAML_DOCS_DIR = os.path.join(os.path.dirname(__file__))

AggregationTest = YamlExamplesTestSuite(
    'AggregationExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/aggregation/*.yaml')).run()
BlueprintsTest = YamlExamplesTestSuite(
    'BlueprintsExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/blueprints/*.yaml')).run()
ElementWiseTest = YamlExamplesTestSuite(
    'ElementwiseExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/elementwise/*.yaml')).run()
ExamplesTest = YamlExamplesTestSuite(
    'ExamplesTest', os.path.join(YAML_DOCS_DIR, '../*.yaml')).run()
IOTest = YamlExamplesTestSuite(
    'IOExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/io/*.yaml')).run()
MLTest = YamlExamplesTestSuite(
    'MLExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/ml/*.yaml')).run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
