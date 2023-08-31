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
from apache_beam.yaml.yaml_transform import LightweightScope
from apache_beam.yaml.yaml_transform import SafeLineLoader
from apache_beam.yaml.yaml_transform import Scope


class ScopeTest(unittest.TestCase):
  def get_scope_by_spec(self, p, spec):
    spec = yaml.load(spec, Loader=SafeLineLoader)

    scope = Scope(
        beam.pvalue.PBegin(p), {},
        spec['transforms'],
        yaml_provider.standard_providers(), {})
    return scope, spec

  def test_get_pcollection_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(range(3))
      scope = Scope(
          p, {'input': elements},
          transforms=[],
          providers=yaml_provider.standard_providers(),
          input_providers={})

      result = scope.get_pcollection('input')
      self.assertEqual("PCollection[Create/Map(decode).None]", str(result))

  def test_get_pcollection_output(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
          - type: Create
            config:
              elements: [0, 1, 3, 4]
          - type: PyMap
            name: Square
            input: Create
            config:
              fn: "lambda x: x*x"
        '''

    scope, spec = self.get_scope_by_spec(p, spec)

    self.assertEqual(
        "PCollection[Create/Map(decode).None]",
        str(scope.get_pcollection("Create")))

    self.assertEqual(
        "PCollection[Square.None]", str(scope.get_pcollection("Square")))

    self.assertEqual(
        "PCollection[Square.None]", str(scope.get_pcollection("PyMap")))

    self.assertTrue(
        scope.get_pcollection("Square") == scope.get_pcollection("PyMap"))

  def test_create_ptransform(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
          - type: PyMap
            config:
              fn: "lambda x: x*x"
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      result = scope.create_ptransform(spec['transforms'][0], [])
      self.assertIsInstance(result, beam.transforms.ParDo)
      self.assertEqual(result.label, 'Map(lambda x: x*x)')

      result_annotations = {**result.annotations()}
      target_annotations = {
          'yaml_type': 'PyMap',
          'yaml_args': '{"fn": "lambda x: x*x"}',
          'yaml_provider': '{"type": "InlineProvider"}'
      }

      # Check if target_annotations is a subset of result_annotations
      self.assertDictEqual(
          result_annotations, {
              **result_annotations, **target_annotations
          })

  def test_create_ptransform_with_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
          - type: PyMap
            config:
              fn: "lambda x: x*x"
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      result = scope.create_ptransform(spec['transforms'][0], [])
      self.assertIsInstance(result, beam.transforms.ParDo)
      self.assertEqual(result.label, 'Map(lambda x: x*x)')

      result_annotations = {
          key: value
          for (key, value) in result.annotations().items()
          if key.startswith('yaml')
      }
      target_annotations = {
          'yaml_type': 'PyMap',
          'yaml_args': '{"fn": "lambda x: x*x"}',
          'yaml_provider': '{"type": "InlineProvider"}'
      }
      self.assertDictEqual(result_annotations, target_annotations)


class LightweightScopeTest(unittest.TestCase):
  @staticmethod
  def get_spec():
    pipeline_yaml = '''
          - type: PyMap
            name: Square
            input: elements
            fn: "lambda x: x * x"
          - type: PyMap
            name: PyMap
            input: elements
            fn: "lambda x: x * x * x"
          - type: Filter
            name: FilterOutBigNumbers
            input: PyMap
            keep: "lambda x: x<100"
          '''
    return yaml.load(pipeline_yaml, Loader=SafeLineLoader)

  def test_init(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    self.assertEqual(len(scope._transforms_by_uuid), 3)
    self.assertCountEqual(
        list(scope._uuid_by_name.keys()),
        ["PyMap", "Square", "Filter", "FilterOutBigNumbers"])

  def test_get_transform_id_and_output_name(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    transform_id, output = scope.get_transform_id_and_output_name("Square")
    self.assertEqual(transform_id, spec[0]['__uuid__'])
    self.assertEqual(output, None)

  def test_get_transform_id_and_output_name_with_dot(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    transform_id, output = \
      scope.get_transform_id_and_output_name("Square.OutputName")
    self.assertEqual(transform_id, spec[0]['__uuid__'])
    self.assertEqual(output, "OutputName")

  def test_get_transform_id_by_uuid(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    transform_id = scope.get_transform_id(spec[0]['__uuid__'])
    self.assertEqual(spec[0]['__uuid__'], transform_id)

  def test_get_transform_id_by_unique_name(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    transform_id = scope.get_transform_id("Square")
    self.assertEqual(transform_id, spec[0]['__uuid__'])

  def test_get_transform_id_by_ambiguous_name(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    with self.assertRaisesRegex(ValueError, r'Ambiguous.*PyMap'):
      scope.get_transform_id(scope.get_transform_id(spec[1]['name']))

  def test_get_transform_id_by_unknown_name(self):
    spec = self.get_spec()
    scope = LightweightScope(spec)
    with self.assertRaisesRegex(ValueError, r'Unknown.*NotExistingTransform'):
      scope.get_transform_id("NotExistingTransform")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
