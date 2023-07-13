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
import unittest

import yaml

import apache_beam as beam
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
from apache_beam.yaml.yaml_transform import SafeLineLoader
from apache_beam.yaml.yaml_transform import Scope
from apache_beam.yaml.yaml_transform import expand_composite_transform
from apache_beam.yaml.yaml_transform import expand_leaf_transform
from apache_beam.yaml.yaml_transform import pipeline_as_composite


class YamlTransformTest(unittest.TestCase):
  def test_only_element(self):
    self.assertEqual(yaml_transform.only_element((1, )), 1)


class SafeLineLoaderTest(unittest.TestCase):
  def test_get_line(self):
    pipeline_yaml = '''
          type: composite
          input:
              elements: input
          transforms:
            - type: PyMap
              name: Square
              input: elements
              fn: "lambda x: x * x"
            - type: PyMap
              name: Cube
              input: elements
              fn: "lambda x: x * x * x"
          output:
              Flatten
          '''
    spec = yaml.load(pipeline_yaml, Loader=SafeLineLoader)
    self.assertEqual(SafeLineLoader.get_line(spec['type']), 2)
    self.assertEqual(SafeLineLoader.get_line(spec['input']), 4)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]), 6)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]['type']), 6)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][0]['name']), 7)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms'][1]), 10)
    self.assertEqual(SafeLineLoader.get_line(spec['output']), 15)
    self.assertEqual(SafeLineLoader.get_line(spec['transforms']), "unknown")

  def test_strip_metadata(self):
    spec_yaml = '''
    transforms:
      - type: PyMap
        name: Square
    '''
    spec = yaml.load(spec_yaml, Loader=SafeLineLoader)
    stripped = SafeLineLoader.strip_metadata(spec['transforms'])

    self.assertFalse(hasattr(stripped[0], '__line__'))
    self.assertFalse(hasattr(stripped[0], '__uuid__'))

  def test_strip_metadata_nothing_to_strip(self):
    spec_yaml = 'prop: 123'
    spec = yaml.load(spec_yaml, Loader=SafeLineLoader)
    stripped = SafeLineLoader.strip_metadata(spec['prop'])

    self.assertFalse(hasattr(stripped, '__line__'))
    self.assertFalse(hasattr(stripped, '__uuid__'))


def new_pipeline():
  return beam.Pipeline(
      options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle'))


class MainTest(unittest.TestCase):
  def get_scope_by_spec(self, p, spec, inputs=None):
    if inputs is None:
      inputs = {}
    spec = yaml.load(spec, Loader=SafeLineLoader)

    scope = Scope(
        beam.pvalue.PBegin(p),
        inputs,
        spec['transforms'],
        yaml_provider.standard_providers(), {})
    return scope, spec

  def test_expand_leaf_transform_with_input(self):
    with new_pipeline() as p:
      spec = '''
          transforms:
          - type: Create
            elements: [0]
          - type: PyMap
            input: Create
            fn: "lambda x: x*x"
          '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_leaf_transform(spec['transforms'][1], scope)
      self.assertTrue('out' in result)
      self.assertIsInstance(result['out'], beam.PCollection)
      self.assertRegex(
          str(result['out'].producer.inputs[0]), r"PCollection.*Create/Map.*")

  def test_expand_leaf_transform_without_input(self):
    with new_pipeline() as p:
      spec = '''
          transforms:
          - type: Create
            elements: [0]
          '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_leaf_transform(spec['transforms'][0], scope)
      self.assertTrue('out' in result)
      self.assertIsInstance(result['out'], beam.PCollection)

  def test_pipeline_as_composite_with_type_transforms(self):
    spec = '''
      type: composite
      transforms:
      - type: Create
        elements: [0,1,2]
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)

  def test_pipeline_as_composite_with_transforms(self):
    spec = '''
      transforms:
      - type: Create
        elements: [0,1,2]
      - type: PyMap
        fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)

  def test_pipeline_as_composite_list(self):
    spec = '''
        - type: Create
          elements: [0,1,2]
        - type: PyMap
          fn: 'lambda x: x*x'
      '''
    spec = yaml.load(spec, Loader=SafeLineLoader)
    result = pipeline_as_composite(spec)

    self.assertEqual(result['type'], 'composite')
    self.assertEqual(result['name'], None)
    self.assertEqual(result['transforms'], spec)
    self.assertTrue('__line__' in result)
    self.assertTrue('__uuid__' in result)

  def test_expand_composite_transform_with_name(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        name: Custom
        transforms:
          - type: Create
            elements: [0,1,2]
        output: 
          Create
              
        '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_composite_transform(spec, scope)
      self.assertRegex(
          str(result['output']), r"PCollection.*Custom/Create/Map.*")

  def test_expand_composite_transform_with_name_input(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        input: elements
        transforms:
          - type: PyMap
            input: input
            fn: 'lambda x: x*x'
        output: 
          PyMap
        '''
      elements = p | beam.Create(range(3))
      scope, spec = self.get_scope_by_spec(p, spec,
                                           inputs={'elements': elements})
      result = expand_composite_transform(spec, scope)

      self.assertRegex(str(result['output']), r"PCollection.*Composite/Map.*")

  def test_expand_composite_transform_root(self):
    with new_pipeline() as p:
      spec = '''
        type: composite
        transforms:
          - type: Create
            elements: [0,1,2]
        output: 
          Create
              
        '''
      scope, spec = self.get_scope_by_spec(p, spec)
      result = expand_composite_transform(spec, scope)
      self.assertRegex(str(result['output']), r"PCollection.*Create/Map.*")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
