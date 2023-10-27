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

"""Runs the examples from the README.md file."""

import argparse
import logging
import os
import random
import re
import sys
import tempfile
import unittest

import mock
import yaml
from yaml.loader import SafeLoader

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import trivial_inference
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform


class FakeSql(beam.PTransform):
  def __init__(self, query):
    self.query = query

  def default_label(self):
    return 'Sql'

  def expand(self, inputs):
    if isinstance(inputs, beam.PCollection):
      inputs = {'PCOLLECTION': inputs}
    # This only handles the most basic of queries, trying to infer the output
    # schema...
    m = re.match('select (.*?) from', self.query, flags=re.IGNORECASE)
    if not m:
      raise ValueError(self.query)

    def guess_name_and_type(expr):
      expr = expr.strip()
      parts = expr.split()
      if len(parts) >= 2 and parts[-2].lower() == 'as':
        name = parts[-1]
      elif re.match(r'[\w.]+', parts[0]):
        name = parts[0].split('.')[-1]
      else:
        name = f'expr{hash(expr)}'
      if '(' in expr:
        expr = expr.lower()
        if expr.startswith('count'):
          typ = int
        elif expr.startswith('avg'):
          typ = float
        else:
          typ = str
      elif '+' in expr:
        typ = float
      else:
        part = parts[0]
        if '.' in part:
          table, field = part.split('.')
          typ = inputs[table].element_type.get_type_for(field)
        else:
          typ = next(iter(inputs.values())).element_type.get_type_for(name)
        # Handle optionals more gracefully.
        if (str(typ).startswith('typing.Union[') or
            str(typ).startswith('typing.Optional[')):
          if len(typ.__args__) == 2 and type(None) in typ.__args__:
            typ, = [t for t in typ.__args__ if t is not type(None)]
      return name, typ

    if m.group(1) == '*':
      return inputs['PCOLLECTION'] | beam.Filter(lambda _: True)
    else:
      output_schema = [
          guess_name_and_type(expr) for expr in m.group(1).split(',')
      ]
      output_element = beam.Row(**{name: typ() for name, typ in output_schema})
      return next(iter(inputs.values())) | beam.Map(
          lambda _: output_element).with_output_types(
              trivial_inference.instance_to_type(output_element))


class FakeReadFromPubSub(beam.PTransform):
  def __init__(self, topic):
    pass

  def expand(self, p):
    data = p | beam.Create([beam.Row(col1='a', col2=1, col3=0.5)])
    result = data | beam.Map(
        lambda row: beam.transforms.window.TimestampedValue(row, 0))
    # TODO(robertwb): Allow this to be inferred.
    result.element_type = data.element_type
    return result


class FakeWriteToPubSub(beam.PTransform):
  def __init__(self, topic):
    pass

  def expand(self, pcoll):
    return pcoll


class SomeAggregation(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.GroupBy(lambda _: 'key').aggregate_field(
        lambda _: 1, sum, 'count')


RENDER_DIR = None
TEST_TRANSFORMS = {
    'Sql': FakeSql,
    'ReadFromPubSub': FakeReadFromPubSub,
    'WriteToPubSub': FakeWriteToPubSub,
    'SomeAggregation': SomeAggregation,
}


class TestProvider(yaml_provider.InlineProvider):
  def _affinity(self, other):
    # Always try to choose this one.
    return float('inf')


class TestEnvironment:
  def __enter__(self):
    self.tempdir = tempfile.TemporaryDirectory()
    return self

  def input_file(self, name, content):
    path = os.path.join(self.tempdir.name, name)
    with open(path, 'w') as fout:
      fout.write(content)
    return path

  def input_csv(self):
    return self.input_file('input.csv', 'col1,col2,col3\nabc,1,2.5\n')

  def input_json(self):
    return self.input_file(
        'input.json', '{"col1": "abc", "col2": 1, "col3": 2.5"}\n')

  def output_file(self):
    return os.path.join(
        self.tempdir.name, str(random.randint(0, 1000)) + '.out')

  def __exit__(self, *args):
    self.tempdir.cleanup()


def replace_recursive(spec, transform_type, arg_name, arg_value):
  if isinstance(spec, dict):
    spec = {
        key: replace_recursive(value, transform_type, arg_name, arg_value)
        for (key, value) in spec.items()
    }
    if spec.get('type', None) == transform_type:
      spec['config'][arg_name] = arg_value
    return spec
  elif isinstance(spec, list):
    return [
        replace_recursive(value, transform_type, arg_name, arg_value)
        for value in spec
    ]
  else:
    return spec


def create_test_method(test_type, test_name, test_yaml):
  test_yaml = test_yaml.replace('pkg.module.fn', 'str')

  def test(self):
    with TestEnvironment() as env:
      spec = yaml.load(test_yaml, Loader=SafeLoader)
      if test_type == 'PARSE':
        return
      if 'ReadFromCsv' in test_yaml:
        spec = replace_recursive(spec, 'ReadFromCsv', 'path', env.input_csv())
      if 'ReadFromJson' in test_yaml:
        spec = replace_recursive(spec, 'ReadFromJson', 'path', env.input_json())
      for write in ['WriteToText', 'WriteToCsv', 'WriteToJson']:
        if write in test_yaml:
          spec = replace_recursive(spec, write, 'path', env.output_file())
      modified_yaml = yaml.dump(spec)
      options = {
          'pickle_library': 'cloudpickle',
          'yaml_experimental_features': ['Combine']
      }
      if RENDER_DIR is not None:
        options['runner'] = 'apache_beam.runners.render.RenderRunner'
        options['render_output'] = [
            os.path.join(RENDER_DIR, test_name + '.png')
        ]
        options['render_leaf_composite_nodes'] = ['.*']
      test_provider = TestProvider(TEST_TRANSFORMS)
      with mock.patch(
          'apache_beam.yaml.yaml_provider.SqlBackedProvider.sql_provider',
          lambda self: test_provider):
        p = beam.Pipeline(options=PipelineOptions(**options))
        yaml_transform.expand_pipeline(
            p, modified_yaml, yaml_provider.merge_providers([test_provider]))
      if test_type == 'BUILD':
        return
      p.run().wait_until_finish()

  return test


def parse_test_methods(markdown_lines):
  # pylint: disable=too-many-nested-blocks
  code_lines = None
  for ix, line in enumerate(markdown_lines):
    line = line.rstrip()
    if line == '```':
      if code_lines is None:
        code_lines = []
        test_type = 'RUN'
        test_name = f'test_line_{ix + 2}'
      else:
        if code_lines:
          if code_lines[0].startswith('- type:'):
            # Treat this as a fragment of a larger pipeline.
            # pylint: disable=not-an-iterable
            code_lines = [
                'pipeline:',
                '  type: chain',
                '  transforms:',
                '    - type: ReadFromCsv',
                '      config:',
                '        path: whatever',
            ] + ['    ' + line for line in code_lines]
          if code_lines[0] == 'pipeline:':
            yaml_pipeline = '\n'.join(code_lines)
            if 'providers:' in yaml_pipeline:
              test_type = 'PARSE'
            yield test_name, create_test_method(
                test_type,
                test_name,
                yaml_pipeline)
        code_lines = None
    elif code_lines is not None:
      code_lines.append(line)


def createTestSuite(name, path):
  with open(path) as readme:
    return type(name, (unittest.TestCase, ), dict(parse_test_methods(readme)))


ReadMeTest = createTestSuite(
    'ReadMeTest', os.path.join(os.path.dirname(__file__), 'README.md'))

ErrorHandlingTest = createTestSuite(
    'ErrorHandlingTest',
    os.path.join(os.path.dirname(__file__), 'yaml_errors.md'))

CombineTest = createTestSuite(
    'CombineTest', os.path.join(os.path.dirname(__file__), 'yaml_combine.md'))

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--render_dir', default=None)
  known_args, unknown_args = parser.parse_known_args(sys.argv)
  if known_args.render_dir:
    RENDER_DIR = known_args.render_dir
  logging.getLogger().setLevel(logging.INFO)
  unittest.main(argv=unknown_args)
