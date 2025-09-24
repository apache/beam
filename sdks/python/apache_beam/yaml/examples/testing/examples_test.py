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
import json
import logging
import os
import random
import re
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
from jinja2 import DictLoader
from jinja2 import Environment
from jinja2 import StrictUndefined

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.typehints.row_type import RowTypeConstraint
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
    topic: Optional[str] = None,
    format: Optional[str] = None,
    schema: Optional[Any] = None,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset_config: Optional[str] = None,
    consumer_config: Optional[Any] = None):
  """
  Mocks the ReadFromKafka transform for testing purposes.

  This PTransform simulates the behavior of the ReadFromKafka transform by
  reading from predefined in-memory data based on the Kafka topic argument.

  Args:
    pcoll: The input PCollection.
    topic: The name of Kafka topic to read from.
    format: The format of the Kafka messages (e.g., 'RAW').
    schema: The schema of the Kafka messages.
    bootstrap_servers: A list of Kafka bootstrap servers to connect to.
    auto_offset_reset_config: A configuration for the auto offset reset.
    consumer_config: A map for additional consumer configuration parameters.

  Returns:
    A PCollection containing the sample data.
  """

  if topic == 'test-topic':
    kafka_byte_messages = KAFKA_TOPICS['test-topic']
    return (
        pcoll
        | beam.Create([msg.decode('utf-8') for msg in kafka_byte_messages])
        | beam.Map(lambda element: beam.Row(payload=element.encode('utf-8'))))

  return None


@beam.ptransform.ptransform_fn
def test_pubsub_read(
    pcoll,
    topic: Optional[str] = None,
    subscription: Optional[str] = None,
    format: Optional[str] = None,
    schema: Optional[Any] = None,
    attributes: Optional[List[str]] = None,
    attributes_map: Optional[str] = None,
    id_attribute: Optional[str] = None,
    timestamp_attribute: Optional[str] = None):
  """
  Mocks the ReadFromPubSub transform for testing purposes.

  This PTransform simulates the behavior of the ReadFromPubSub transform by
  reading from predefined in-memory data based on the Pub/Sub topic argument.
  Args:
    pcoll: The input PCollection.
    topic: The name of Pub/Sub topic to read from.
    subscription: The name of Pub/Sub subscription to read from.
    format: The format of the Pub/Sub messages (e.g., 'JSON').
    schema: The schema of the Pub/Sub messages.
    attributes: A list of attributes to include in the output.
    attributes_map: A string representing a mapping of attributes.
    id_attribute: The attribute to use as the ID for the message.
    timestamp_attribute: The attribute to use as the timestamp for the message.

  Returns:
    A PCollection containing the sample data.
  """

  if topic == 'test-topic':
    pubsub_messages = PUBSUB_TOPICS['test-topic']
    return (
        pcoll
        | beam.Create([json.loads(msg.data) for msg in pubsub_messages])
        | beam.Map(lambda element: beam.Row(**element)))
  elif topic == 'taxi-ride-topic':
    pubsub_messages = PUBSUB_TOPICS['taxi-ride-topic']
    schema = input_data.TaxiRideEventSchema
    return (
        pcoll
        | beam.Create([json.loads(msg.data) for msg in pubsub_messages])
        |
        beam.Map(lambda element: beam.Row(**element)).with_output_types(schema))

  return None


@beam.ptransform.ptransform_fn
def test_run_inference_taxi_fare(pcoll, inference_tag, model_handler):
  """
  This PTransform simulates the behavior of the RunInference transform.

  Args:
    pcoll: The input PCollection.
    inference_tag: The tag to use for the returned inference.
    model_handler: A configuration for the respective ML model handler

  Returns:
    A PCollection containing the enriched data.
  """
  def _fn(row):
    input = row._asdict()

    row = {inference_tag: PredictionResult(input, 10.0), **input}

    return beam.Row(**row)

  schema = _format_predicition_result_ouput(pcoll, inference_tag)
  return pcoll | beam.Map(_fn).with_output_types(schema)


@beam.ptransform.ptransform_fn
def test_run_inference_youtube_comments(pcoll, inference_tag, model_handler):
  """
  This PTransform simulates the behavior of the RunInference transform.

  Args:
    pcoll: The input PCollection.
    inference_tag: The tag to use for the returned inference.
    model_handler: A configuration for the respective ML model handler

  Returns:
    A PCollection containing the enriched data.
  """
  def _fn(row):
    input = row._asdict()

    row = {
        inference_tag: PredictionResult(
            input['comment_text'],
            [{
                'label': 'POSITIVE'
                if 'happy' in input['comment_text'] else 'NEGATIVE',
                'score': 0.95
            }]),
        **input
    }

    return beam.Row(**row)

  schema = _format_predicition_result_ouput(pcoll, inference_tag)
  return pcoll | beam.Map(_fn).with_output_types(schema)


def _format_predicition_result_ouput(pcoll, inference_tag):
  user_type = RowTypeConstraint.from_user_type(pcoll.element_type.user_type)
  user_schema_fields = [(name, type(typ) if not isinstance(typ, type) else typ)
                        for (name,
                             typ) in user_type._fields] if user_type else []
  inference_output_type = RowTypeConstraint.from_fields([
      ('example', Any), ('inference', Any), ('model_id', Optional[str])
  ])
  return RowTypeConstraint.from_fields(
      user_schema_fields + [(str(inference_tag), inference_output_type)])


TEST_PROVIDERS = {
    'TestEnrichment': test_enrichment,
    'TestReadFromKafka': test_kafka_read,
    'TestReadFromPubSub': test_pubsub_read,
    'TestRunInferenceYouTubeComments': test_run_inference_youtube_comments,
    'TestRunInferenceTaxiFare': test_run_inference_taxi_fare,
}
"""
Transforms not requiring inputs.
"""
INPUT_TRANSFORM_TEST_PROVIDERS = ['TestReadFromKafka', 'TestReadFromPubSub']


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
    expected = [line for line in expected if line]

    raw_spec_string = ''.join(lines)
    # Filter for any jinja preprocessor - this has to be done before other
    # preprocessors.
    jinja_preprocessor = [
        preprocessor for preprocessor in custom_preprocessors
        if 'jinja_preprocessor' in preprocessor.__name__
    ]
    if jinja_preprocessor:
      jinja_preprocessor = jinja_preprocessor[0]
      raw_spec_string = jinja_preprocessor(
          raw_spec_string, self._testMethodName)
      custom_preprocessors.remove(jinja_preprocessor)

    pipeline_spec = yaml.load(
        raw_spec_string, Loader=yaml_transform.SafeLineLoader)

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

  def _python_deps_involved(spec_filename):
    return any(
        substr in spec_filename for substr in
        ['deps', 'streaming_sentiment_analysis', 'ml_preprocessing'])

  def _java_deps_involved(spec_filename):
    return any(
        substr in spec_filename
        for substr in ['java_deps', 'streaming_taxifare_prediction'])

  if _python_deps_involved(pipeline_spec_file):
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

  if _java_deps_involved(pipeline_spec_file):
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


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_wordcount_minimal_yaml'])
def _wordcount_minimal_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for the wordcount_minimal.yaml test.

  This preprocessor generates a random input file based on the expected output
  of the wordcount example. This allows the test to verify the pipeline's
  correctness without relying on a fixed input file.

  Based on this expected output: #  Row(word='king', count=311)

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

  return _wordcount_random_shuffler(test_spec, all_words, env)


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_wordCountInclude_yaml', 'test_wordCountImport_yaml'])
def _wordcount_jinja_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for the wordcount Jinja tests.

  This preprocessor generates a random input file based on the expected output
  of the wordcount example. This allows the test to verify the pipeline's
  correctness without relying on a fixed input file.

  Based on this expected output: #  Row(output='king - 311')

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
    match = re.search(r"output='(.*) - (\d+)'", element)
    if match:
      word, count_str = match.groups()
      all_words += [word] * int(count_str)
  return _wordcount_random_shuffler(test_spec, all_words, env)


def _wordcount_random_shuffler(
    test_spec: dict, all_words: List[str], env: TestEnvironment):
  """
  Helper function to create a randomized input file for wordcount-style tests.

  This function takes a list of words, shuffles them, and arranges them into
  randomly sized lines. It then creates a temporary input file with this
  content and updates the provided test specification to use this file as
  the input for a 'ReadFromText' transform.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    all_words: A list of strings, where each string is a word to be included
      in the generated input file.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with the input file path for
    'ReadFromText' replaced with the path to the newly generated file.
  """
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


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_kafka_yaml', 'test_kafka_to_iceberg_yaml'])
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
        transform['config']['topic'] = 'test-topic'

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_simple_filter_yaml',
    'test_simple_filter_and_combine_yaml',
    'test_iceberg_read_yaml',
    'test_iceberg_write_yaml',
    'test_kafka_yaml',
    'test_spanner_read_yaml',
    'test_spanner_write_yaml',
    'test_enrich_spanner_with_bigquery_yaml',
    'test_pubsub_topic_to_bigquery_yaml',
    'test_pubsub_subscription_to_bigquery_yaml',
    'test_jdbc_to_bigquery_yaml',
    'test_spanner_to_avro_yaml',
    'test_gcs_text_to_bigquery_yaml',
    'test_sqlserver_to_bigquery_yaml',
    'test_postgres_to_bigquery_yaml',
    'test_kafka_to_iceberg_yaml',
    'test_pubsub_to_iceberg_yaml',
    'test_oracle_to_bigquery_yaml',
    'test_mysql_to_bigquery_yaml',
    'test_spanner_to_bigquery_yaml',
    'test_streaming_sentiment_analysis_yaml',
    'test_iceberg_migration_yaml',
    'test_ml_preprocessing_yaml',
    'test_anomaly_scoring_yaml',
    'test_wordCountInclude_yaml',
    'test_wordCountImport_yaml',
    'test_iceberg_to_alloydb_yaml'
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


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_simple_filter_yaml',
    'test_simple_filter_and_combine_yaml',
    'test_gcs_text_to_bigquery_yaml'
])
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


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_iceberg_read_yaml', 'test_iceberg_to_alloydb_yaml'])
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


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_spanner_read_yaml',
    'test_enrich_spanner_with_bigquery_yaml',
    'test_spanner_to_avro_yaml',
    'test_spanner_to_bigquery_yaml'
])
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


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_pubsub_topic_to_bigquery_yaml',
    'test_pubsub_subscription_to_bigquery_yaml',
    'test_pubsub_to_iceberg_yaml'
])
def _pubsub_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from Pub/Sub.
  This preprocessor replaces any ReadFromPubSub transform with a Create
  transform that reads from a predefined in-memory list of messages.
  This allows the test to verify the pipeline's correctness without relying
  on an active Pub/Sub subscription or topic.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'ReadFromPubSub':
        transform['type'] = 'TestReadFromPubSub'
        transform['config']['topic'] = 'test-topic'

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_jdbc_to_bigquery_yaml',
])
def _jdbc_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from generic Jdbc.
  url syntax: 'jdbc:<database-type>://<host>:<port>/<database>'
  """
  return _db_io_read_test_processor(
      test_spec, lambda url: url.split('/')[-1], 'Jdbc')


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_sqlserver_to_bigquery_yaml',
])
def __sqlserver_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from SqlServer.
  url syntax: 'jdbc:sqlserver://<host>:<port>;databaseName=<database>;
    user=<user>;password=<password>;encrypt=false;trustServerCertificate=true'
  """
  return _db_io_read_test_processor(
      test_spec, lambda url: url.split(';')[1].split('=')[-1], 'SqlServer')


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_postgres_to_bigquery_yaml',
])
def __postgres_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from Postgres.
  url syntax: 'jdbc:postgresql://<host>:<port>/shipment?user=<user>&
    password=<password>'
  """
  return _db_io_read_test_processor(
      test_spec, lambda url: url.split('/')[3].split('?')[0], 'Postgres')


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_oracle_to_bigquery_yaml',
])
def __oracle_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from Oracle.
  url syntax: 'jdbc:oracle:thin:system/oracle@<host>:{port}/<database>'
  """
  return _db_io_read_test_processor(
      test_spec, lambda url: url.split('/')[2], 'Oracle')


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_mysql_to_bigquery_yaml',
])
def __mysql_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve reading from MySql.
  url syntax: 'jdbc:mysql://<host>:<port>/<database>?user=<user>&
    password=<password>'
  """
  return _db_io_read_test_processor(
      test_spec, lambda url: url.split('/')[3].split('?')[0], 'MySql')


def _db_io_read_test_processor(
    test_spec: dict, database_url_fn: Callable, database_type: str):
  """
  This preprocessor replaces any ReadFrom<database> transform with a Create
  transform that reads from a predefined in-memory list of records. This allows
  the test to verify the pipeline's correctness without relying on an active
  database.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      transform_name = f"ReadFrom{database_type}"
      if transform.get('type', '').startswith(transform_name):
        config = transform['config']
        url = config['url']
        database = database_url_fn(url)
        if (table := config.get('table', None)) is None:
          table = config.get('query', '').split('FROM')[-1].strip()
        transform['type'] = 'Create'
        transform['config'] = {
            k: v
            for k, v in config.items() if k.startswith('__')
        }
        elements = INPUT_TABLES[(database_type, database, table)]
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
    'test_streaming_sentiment_analysis_yaml')
def _streaming_sentiment_analysis_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve the streaming sentiment analysis example.

  This preprocessor replaces several IO transforms and the RunInference
  transform.
  This allows the test to verify the pipeline's correctness without relying on
  external data sources and the model hosted on VertexAI.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with ... transforms replaced.
  """
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'PyTransform' and transform.get(
          'name', '') == 'ReadFromGCS':
        transform['windowing'] = {'type': 'fixed', 'size': '30s'}

        file_name = 'youtube-comments.csv'
        local_path = env.input_file(file_name, INPUT_FILES[file_name])
        transform['config']['kwargs']['file_pattern'] = local_path

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'ReadFromKafka':
        config = transform['config']
        transform['type'] = 'ReadFromCsv'
        transform['config'] = {
            k: v
            for k, v in config.items() if k.startswith('__')
        }
        transform['config']['path'] = ""

        file_name = 'youtube-comments.csv'
        test_spec = replace_recursive(
            test_spec,
            transform['type'],
            'path',
            env.input_file(file_name, INPUT_FILES[file_name]))

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'RunInference':
        transform['type'] = 'TestRunInferenceYouTubeComments'

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(
    'test_streaming_taxifare_prediction_yaml')
def _streaming_taxifare_prediction_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve the streaming taxi fare prediction
  example.

  This preprocessor replaces several IO transforms and the RunInference
  transform. This allows the test to verify the pipeline's correctness
  without relying on external data sources and the model hosted on VertexAI.
  It also turns this non-linear pipeline into a linear pipeline by replacing
  the ReadFromKafka and WriteToKafka transforms with MapToFields and linking
  the two disconnected pipeline components together. The pipeline logic,
  however, remains the same and is still being tested accordingly.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with several involved IO transforms and
    the RunInference transform replaced.
  """

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '') == 'ReadFromPubSub':
        transform['type'] = 'TestReadFromPubSub'
        transform['config']['topic'] = 'taxi-ride-topic'

      elif transform.get('type', '') == 'WriteToKafka':
        transform['type'] = 'MapToFields'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if k.startswith('__')
        }
        transform['config']['fields'] = {
            'ride_id': 'ride_id',
            'pickup_longitude': 'pickup_longitude',
            'pickup_latitude': 'pickup_latitude',
            'pickup_datetime': 'pickup_datetime',
            'dropoff_longitude': 'dropoff_longitude',
            'dropoff_latitude': 'dropoff_latitude',
            'passenger_count': 'passenger_count',
        }

      elif transform.get('type', '') == 'ReadFromKafka':
        transform['type'] = 'MapToFields'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if k.startswith('__')
        }
        transform['input'] = 'WriteKafka'
        transform['config']['fields'] = {
            'ride_id': 'ride_id',
            'pickup_longitude': 'pickup_longitude',
            'pickup_latitude': 'pickup_latitude',
            'pickup_datetime': 'pickup_datetime',
            'dropoff_longitude': 'dropoff_longitude',
            'dropoff_latitude': 'dropoff_latitude',
            'passenger_count': 'passenger_count',
        }

      elif transform.get('type', '') == 'WriteToBigQuery':
        transform['type'] = 'LogForTesting'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if (k.startswith('__') or k == 'error_handling')
        }

      elif transform.get('type', '') == 'RunInference':
        transform['type'] = 'TestRunInferenceTaxiFare'

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_iceberg_migration_yaml',
    'test_ml_preprocessing_yaml',
    'test_anomaly_scoring_yaml'
])
def _batch_log_analysis_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):
  """
  Preprocessor for tests that involve the batch log analysis example.

  This preprocessor replaces several IO transforms and the MLTransform.
  This allows the test to verify the pipeline's correctness
  without relying on external data sources or MLTransform's many dependencies.

  Args:
    test_spec: The dictionary representation of the YAML pipeline specification.
    expected: A list of strings representing the expected output of the
      pipeline.
    env: The TestEnvironment object providing utilities for creating temporary
      files.

  Returns:
    The modified test_spec dictionary with ReadFromText transforms replaced.
  """

  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      # Mock ReadFromCsv in iceberg_migration.yaml pipeline
      if transform.get('type', '') == 'ReadFromCsv':
        file_name = 'system-logs.csv'
        local_path = env.input_file(file_name, INPUT_FILES[file_name])
        transform['config']['path'] = local_path

      # Mock ReadFromIceberg in ml_preprocessing.yaml pipeline
      elif transform.get('type', '') == 'ReadFromIceberg':
        transform['type'] = 'Create'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if (k.startswith('__'))
        }

        transform['config']['elements'] = input_data.system_logs_data()

      # Mock MLTransform in ml_preprocessing.yaml pipeline
      elif transform.get('type', '') == 'MLTransform':
        transform['type'] = 'MapToFields'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if k.startswith('__')
        }

        transform['config']['language'] = 'python'
        transform['config']['fields'] = {
            'LineId': 'LineId',
            'Date': 'Date',
            'Time': 'Time',
            'Level': 'Level',
            'Process': 'Process',
            'Component': 'Component',
            'Content': 'Content',
            'embedding': {
                'callable': f"lambda row: {input_data.embedding_data()}",
            }
        }

      # Mock MapToFields in ml_preprocessing.yaml pipeline
      elif transform.get('type', '') == 'MapToFields' and \
          transform.get('name', '') == 'Normalize':
        transform['config']['dependencies'] = ['numpy']

      # Mock ReadFromBigQuery in anomaly_scoring.yaml pipeline
      elif transform.get('type', '') == 'ReadFromBigQuery':
        transform['type'] = 'Create'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if (k.startswith('__'))
        }

        transform['config']['elements'] = (
            input_data.system_logs_embedding_data())

      # Mock PyTransform in anomaly_scoring.yaml pipeline
      elif transform.get('type', '') == 'PyTransform' and \
          transform.get('name', '') == 'AnomalyScoring':
        transform['type'] = 'MapToFields'
        transform['config'] = {
            k: v
            for (k, v) in transform.get('config', {}).items()
            if k.startswith('__')
        }

        transform['config']['language'] = 'python'
        transform['config']['fields'] = {
            'example': 'embedding',
            'predictions': {
                'callable': """lambda row: [{
                  'score': 0.65,
                  'label': 0,
                  'threshold': 0.8
              }]""",
            }
        }

  return test_spec


@YamlExamplesTestSuite.register_test_preprocessor(
    ['test_wordCountInclude_yaml', 'test_wordCountImport_yaml'])
def _jinja_preprocessor(raw_spec_string: str, test_name: str):
  """
  Preprocessor for Jinja-based YAML tests.

  This function takes a raw YAML string, which is treated as a Jinja2
  template, and renders it to produce the final pipeline specification.
  It specifically handles templates that use the `{% include ... %}`
  directive by manually loading the content of the included files from the
  filesystem.

  The Jinja variables required for rendering are loaded from a predefined
  data source.

  Args:
    raw_spec_string: A string containing the raw YAML content, which is a
      Jinja2 template.

  Returns:
    A string containing the fully rendered YAML pipeline specification.
  """
  jinja_variables = json.loads(input_data.word_count_jinja_parameter_data())
  test_file_dir = os.path.dirname(__file__)
  sdk_root = os.path.abspath(os.path.join(test_file_dir, '../../../..'))

  include_files = input_data.word_count_jinja_template_data(test_name)
  mock_templates = {'main_template': raw_spec_string}
  for file_path in include_files:
    full_path = os.path.join(sdk_root, file_path)
    with open(full_path, 'r', encoding='utf-8') as f:
      mock_templates[file_path] = f.read()

  # Can't use the standard expand_jinja method due to it not supporting
  # `% include` jinja templization.
  # TODO(#35936): Maybe update expand_jinja to handle this case.
  jinja_env = Environment(
      loader=DictLoader(mock_templates), undefined=StrictUndefined)
  template = jinja_env.get_template('main_template')
  rendered_yaml_string = template.render(jinja_variables)
  return rendered_yaml_string


INPUT_FILES = {
    'products.csv': input_data.products_csv(),
    'kinglear.txt': input_data.text_data(),
    'youtube-comments.csv': input_data.youtube_comments_csv(),
    'system-logs.csv': input_data.system_logs_csv()
}

KAFKA_TOPICS = {'test-topic': input_data.kafka_messages_data()}

PUBSUB_TOPICS = {
    'test-topic': input_data.pubsub_messages_data(),
    'taxi-ride-topic': input_data.pubsub_taxi_ride_events_data()
}

INPUT_TABLES = {
    ('shipment-test', 'shipment', 'shipments'): input_data.shipments_data(),
    ('orders-test', 'order-database', 'orders'): input_data.
    spanner_orders_data(),
    ('db', 'users', 'NY'): input_data.iceberg_dynamic_destinations_users_data(),
    ('BigTable', 'beam-test', 'bigtable-enrichment-test'): input_data.
    bigtable_data(),
    ('BigQuery', 'ALL_TEST', 'customers'): input_data.bigquery_data(),
    ('Jdbc', 'shipment', 'shipments'): input_data.shipments_data(),
    ('SqlServer', 'shipment', 'shipments'): input_data.shipments_data(),
    ('Postgres', 'shipment', 'shipments'): input_data.shipments_data(),
    ('Oracle', 'shipment', 'shipments'): input_data.shipments_data(),
    ('MySql', 'shipment', 'shipments'): input_data.shipments_data()
}
YAML_DOCS_DIR = os.path.join(os.path.dirname(__file__))

AggregationTest = YamlExamplesTestSuite(
    'AggregationExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/aggregation/*.yaml')).run()
BlueprintTest = YamlExamplesTestSuite(
    'BlueprintExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/blueprint/*.yaml')).run()
ElementWiseTest = YamlExamplesTestSuite(
    'ElementwiseExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/elementwise/*.yaml')).run()
ExamplesTest = YamlExamplesTestSuite(
    'ExamplesTest', os.path.join(YAML_DOCS_DIR, '../*.yaml')).run()
JinjaTest = YamlExamplesTestSuite(
    'JinjaExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/jinja/**/*.yaml')).run()
IOTest = YamlExamplesTestSuite(
    'IOExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/io/*.yaml')).run()
MLTest = YamlExamplesTestSuite(
    'MLExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/ml/**/*.yaml')).run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
