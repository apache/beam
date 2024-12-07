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
import unittest
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from unittest import mock

import yaml

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
from apache_beam.yaml.readme_test import TestEnvironment
from apache_beam.yaml.readme_test import replace_recursive


# Used to simulate Enrichment transform during tests
# The GitHub action that invokes these tests does not
# have gcp dependencies installed which is a prerequisite
# to apache_beam.transforms.enrichment.Enrichment as a top-level
# import.
@beam.ptransform.ptransform_fn
def test_enrichment(
    pcoll,
    enrichment_handler: str,
    handler_config: Dict[str, Any],
    timeout: Optional[float] = 30):
  if enrichment_handler == 'BigTable':
    row_key = handler_config['row_key']
    bt_data = INPUT_TABLES[(
        'BigTable', handler_config['instance_id'], handler_config['table_id'])]
    products = {str(data[row_key]): data for data in bt_data}

    def _fn(row):
      left = row._asdict()
      right = products[str(row[row_key])]
      left['product'] = left.get('product', None) or right
      return beam.Row(**row)
  elif enrichment_handler == 'BigQuery':
    row_key = handler_config['fields']
    dataset, table = handler_config['table_name'].split('.')[-2:]
    bq_data = INPUT_TABLES[('BigQuery', str(dataset), str(table))]
    products = {
        tuple(str(data[key]) for key in row_key): data
        for data in bq_data
    }

    def _fn(row):
      left = row._asdict()
      right = products[tuple(str(left[k]) for k in row_key)]
      row = {
          key: left.get(key, None) or right[key]
          for key in {*left.keys(), *right.keys()}
      }
      return beam.Row(**row)

  else:
    raise ValueError(f'{enrichment_handler} is not a valid enrichment_handler.')

  return pcoll | beam.Map(_fn)


TEST_PROVIDERS = {
    'TestEnrichment': test_enrichment,
}


def check_output(expected: List[str]):
  def _check_inner(actual: List[PCollection[str]]):
    formatted_actual = actual | beam.Flatten() | beam.Map(
        lambda row: str(beam.Row(**row._asdict())))
    assert_matches_stdout(formatted_actual, expected)

  return _check_inner


def products_csv():
  return '\n'.join([
      'transaction_id,product_name,category,price',
      'T0012,Headphones,Electronics,59.99',
      'T5034,Leather Jacket,Apparel,109.99',
      'T0024,Aluminum Mug,Kitchen,29.99',
      'T0104,Headphones,Electronics,59.99',
      'T0302,Monitor,Electronics,249.99'
  ])


def spanner_orders_data():
  return [{
      'order_id': 1,
      'customer_id': 1001,
      'product_id': 2001,
      'order_date': '24-03-24',
      'order_amount': 150,
  },
          {
              'order_id': 2,
              'customer_id': 1002,
              'product_id': 2002,
              'order_date': '19-04-24',
              'order_amount': 90,
          },
          {
              'order_id': 3,
              'customer_id': 1003,
              'product_id': 2003,
              'order_date': '7-05-24',
              'order_amount': 110,
          }]


def spanner_shipments_data():
  return [{
      'shipment_id': 'S1',
      'customer_id': 'C1',
      'shipment_date': '2023-05-01',
      'shipment_cost': 150.0,
      'customer_name': 'Alice',
      'customer_email': 'alice@example.com'
  },
          {
              'shipment_id': 'S2',
              'customer_id': 'C2',
              'shipment_date': '2023-06-12',
              'shipment_cost': 300.0,
              'customer_name': 'Bob',
              'customer_email': 'bob@example.com'
          },
          {
              'shipment_id': 'S3',
              'customer_id': 'C1',
              'shipment_date': '2023-05-10',
              'shipment_cost': 20.0,
              'customer_name': 'Alice',
              'customer_email': 'alice@example.com'
          },
          {
              'shipment_id': 'S4',
              'customer_id': 'C4',
              'shipment_date': '2024-07-01',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          },
          {
              'shipment_id': 'S5',
              'customer_id': 'C5',
              'shipment_date': '2023-05-09',
              'shipment_cost': 300.0,
              'customer_name': 'Erin',
              'customer_email': 'erin@example.com'
          },
          {
              'shipment_id': 'S6',
              'customer_id': 'C4',
              'shipment_date': '2024-07-02',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          }]


def bigtable_data():
  return [{
      'product_id': '1', 'product_name': 'pixel 5', 'product_stock': '2'
  }, {
      'product_id': '2', 'product_name': 'pixel 6', 'product_stock': '4'
  }, {
      'product_id': '3', 'product_name': 'pixel 7', 'product_stock': '20'
  }, {
      'product_id': '4', 'product_name': 'pixel 8', 'product_stock': '10'
  }, {
      'product_id': '5', 'product_name': 'pixel 11', 'product_stock': '3'
  }, {
      'product_id': '6', 'product_name': 'pixel 12', 'product_stock': '7'
  }, {
      'product_id': '7', 'product_name': 'pixel 13', 'product_stock': '8'
  }, {
      'product_id': '8', 'product_name': 'pixel 14', 'product_stock': '3'
  }]


def bigquery_data():
  return [{
      'customer_id': 1001,
      'customer_name': 'Alice',
      'customer_email': 'alice@gmail.com'
  },
          {
              'customer_id': 1002,
              'customer_name': 'Bob',
              'customer_email': 'bob@gmail.com'
          },
          {
              'customer_id': 1003,
              'customer_name': 'Claire',
              'customer_email': 'claire@gmail.com'
          }]


def create_test_method(
    pipeline_spec_file: str,
    custom_preprocessors: List[Callable[..., Union[Dict, List]]]):
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
                pipeline_spec, [yaml_provider.InlineProvider(TEST_PROVIDERS)])
        ]
        if not actual[0]:
          actual = list(p.transforms_stack[0].parts[-1].outputs.values())
          for transform in p.transforms_stack[0].parts[:-1]:
            if transform.transform.label == 'log_for_testing':
              actual += list(transform.outputs.values())
        check_output(expected)(actual)

  return test_yaml_example


class YamlExamplesTestSuite:
  _test_preprocessor: Dict[str, List[Callable[..., Union[Dict, List]]]] = {}

  def __init__(self, name: str, path: str):
    self._test_suite = self.create_test_suite(name, path)

  def run(self):
    return self._test_suite

  @classmethod
  def parse_test_methods(cls, path: str):
    files = glob.glob(path)
    if not files and os.path.exists(path) and os.path.isfile(path):
      files = [path]
    for file in files:
      test_name = f'test_{file.split(os.sep)[-1].replace(".", "_")}'
      custom_preprocessors = cls._test_preprocessor.get(test_name, [])
      yield test_name, create_test_method(file, custom_preprocessors)

  @classmethod
  def create_test_suite(cls, name: str, path: str):
    return type(name, (unittest.TestCase, ), dict(cls.parse_test_methods(path)))

  @classmethod
  def register_test_preprocessor(cls, test_names: Union[str, List]):
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


@YamlExamplesTestSuite.register_test_preprocessor([
    'test_simple_filter_yaml',
    'test_simple_filter_and_combine_yaml',
    'test_spanner_read_yaml',
    'test_spanner_write_yaml',
    'test_enrich_spanner_with_bigquery_yaml'
])
def _io_write_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):

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
    ['test_spanner_read_yaml', 'test_enrich_spanner_with_bigquery_yaml'])
def _spanner_io_read_test_preprocessor(
    test_spec: dict, expected: List[str], env: TestEnvironment):

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
                  ' FROM', maxsplit=1)[0])
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
  if pipeline := test_spec.get('pipeline', None):
    for transform in pipeline.get('transforms', []):
      if transform.get('type', '').startswith('Enrichment'):
        transform['type'] = 'TestEnrichment'

  return test_spec


INPUT_FILES = {'products.csv': products_csv()}
INPUT_TABLES = {
    ('shipment-test', 'shipment', 'shipments'): spanner_shipments_data(),
    ('orders-test', 'order-database', 'orders'): spanner_orders_data(),
    ('BigTable', 'beam-test', 'bigtable-enrichment-test'): bigtable_data(),
    ('BigQuery', 'ALL_TEST', 'customers'): bigquery_data()
}

YAML_DOCS_DIR = os.path.join(os.path.dirname(__file__))
ExamplesTest = YamlExamplesTestSuite(
    'ExamplesTest', os.path.join(YAML_DOCS_DIR, '../*.yaml')).run()

ElementWiseTest = YamlExamplesTestSuite(
    'ElementwiseExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/elementwise/*.yaml')).run()

AggregationTest = YamlExamplesTestSuite(
    'AggregationExamplesTest',
    os.path.join(YAML_DOCS_DIR, '../transforms/aggregation/*.yaml')).run()

IOTest = YamlExamplesTestSuite(
    'IOExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/io/*.yaml')).run()

MLTest = YamlExamplesTestSuite(
    'MLExamplesTest', os.path.join(YAML_DOCS_DIR,
                                   '../transforms/ml/*.yaml')).run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
