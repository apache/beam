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
from apache_beam.yaml.yaml_transform import SafeLineLoader
from apache_beam.yaml import yaml_provider
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
            elements: [0, 1, 3, 4]
          - type: PyMap
            name: Square
            input: Create
            fn: "lambda x: x*x"
        '''

    scope, spec = self.get_scope_by_spec(p, spec)

    result = scope.get_pcollection("Create")
    self.assertEqual("PCollection[Create/Map(decode).None]", str(result))

    result = scope.get_pcollection("Square")
    self.assertEqual("PCollection[Square.None]", str(result))

    result = scope.get_pcollection("PyMap")
    self.assertEqual("PCollection[Square.None]", str(result))

  def test_get_pcollection_ambigous_name(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
            - type: Create
              elements: [0, 1, 3, 4]
            - type: PyMap
              fn: "lambda x: x*x"
            - type: PyMap
              fn: "lambda x: x*x*x"
        '''

      scope, spec = self.get_scope_by_spec(p, spec)

      with self.assertRaisesRegex(ValueError, r'Ambiguous.*'):
        scope.get_pcollection("PyMap")

  def test_unique_name_by_name(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
            - type: Create
              name: MyElements
              elements: [0, 1, 3, 4]
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      spec_transform = spec['transforms'][0]
      p_transform = scope.create_ptransform(spec_transform, [])

      result = scope.unique_name(spec_transform, p_transform)
      self.assertEqual(result, "MyElements")
      self.assertIn("MyElements", scope._seen_names)

      result = scope.unique_name(spec_transform, p_transform)
      self.assertIn("MyElements@3", scope._seen_names)
      self.assertEqual(result, "MyElements@3")

  def test_unique_name_by_label(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
            - type: Create
              elements: [0, 1, 3, 4]
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      spec_transform = spec['transforms'][0]
      p_transform = scope.create_ptransform(spec_transform, [])

      result = scope.unique_name(spec_transform, p_transform)
      self.assertEqual(result, "Create")
      self.assertIn("Create", scope._seen_names)

      result = scope.unique_name(spec_transform, p_transform)
      self.assertIn("Create@3", scope._seen_names)
      self.assertEqual(result, "Create@3")

  def test_unique_name_strict(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
            - type: Create
              name: MyElements
              elements: [0, 1, 3, 4]
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      spec_transform = spec['transforms'][0]
      p_transform = scope.create_ptransform(spec_transform, [])

      result = scope.unique_name(spec_transform, p_transform, strictness=1)
      self.assertEqual(result, "MyElements")
      self.assertIn("MyElements", scope._seen_names)

      with self.assertRaisesRegex(ValueError, r"Duplicate name"):
        scope.unique_name(spec_transform, p_transform, strictness=1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
