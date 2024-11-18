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

import collections
import logging
import unittest

import yaml

import apache_beam as beam
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform
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
          - type: LogForTesting
            name: Square
            input: Create
        '''

    scope, spec = self.get_scope_by_spec(p, spec)

    self.assertEqual(
        "PCollection[Create/Map(decode).None]",
        str(scope.get_pcollection("Create")))

    self.assertEqual(
        "PCollection[Square/LogForTesting.None]",
        str(scope.get_pcollection("Square")))

    self.assertEqual(
        "PCollection[Square/LogForTesting.None]",
        str(scope.get_pcollection("LogForTesting")))

    self.assertTrue(
        scope.get_pcollection("Square") == scope.get_pcollection(
            "LogForTesting"))

  def test_create_ptransform(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      spec = '''
        transforms:
          - type: Create
            config:
              elements: [1, 2, 3]
        '''
      scope, spec = self.get_scope_by_spec(p, spec)

      result = scope.create_ptransform(spec['transforms'][0], [])
      self.assertIsInstance(result, beam.transforms.Create)
      self.assertEqual(result.label, 'Create')

      result_annotations = {**result.annotations()}
      target_annotations = {
          'yaml_type': 'Create',
          'yaml_args': '{"elements": [1, 2, 3]}',
          'yaml_provider': '{"type": "InlineProvider"}'
      }

      # Check if target_annotations is a subset of result_annotations
      self.assertDictEqual(
          result_annotations, {
              **result_annotations, **target_annotations
          })


class TestProvider(yaml_provider.InlineProvider):
  def __init__(self, transform, name):
    super().__init__({
        name: lambda: beam.Map(lambda x: (x or ()) + (name, )),  # or None
        transform: lambda: beam.Map(lambda x: (x or ()) + (name, )),
    })
    self._transform = transform
    self._name = name

  def __repr__(self):
    return 'TestProvider(%r, %r)' % (self._transform, self._name)

  def _affinity(self, other):
    if isinstance(other, TestProvider):
      # Providers are closer based on how much their names match prefixes.
      affinity = 1
      for x, y in zip(self._name, other._name):
        if x != y:
          break
        affinity *= 10
      return affinity
    else:
      return -1000


class ProviderAffinityTest(unittest.TestCase):
  @staticmethod
  def create_scope(s, providers):
    providers_dict = collections.defaultdict(list)
    for provider in providers:
      for transform_type in provider.provided_transforms():
        providers_dict[transform_type].append(provider)
    spec = yaml_transform.preprocess(yaml.load(s, Loader=SafeLineLoader))
    return Scope(
        None, {},
        transforms=spec['transforms'],
        providers=providers_dict,
        input_providers={})

  def test_best_provider_based_on_input(self):
    provider_Ax = TestProvider('A', 'xxx')
    provider_Ay = TestProvider('A', 'yyy')
    provider_Bx = TestProvider('B', 'xxz')
    provider_By = TestProvider('B', 'yyz')
    scope = self.create_scope(
        '''
        type: chain
        transforms:
          - type: A
          - type: B
        ''', [provider_Ax, provider_Ay, provider_Bx, provider_By])
    self.assertEqual(scope.best_provider('B', [provider_Ax]), provider_Bx)
    self.assertEqual(scope.best_provider('B', [provider_Ay]), provider_By)

  def test_best_provider_based_on_followers(self):
    close_provider = TestProvider('A', 'xxy')
    far_provider = TestProvider('A', 'yyy')
    following_provider = TestProvider('B', 'xxx')
    scope = self.create_scope(
        '''
        type: chain
        transforms:
          - type: A
          - type: B
        ''', [far_provider, close_provider, following_provider])
    self.assertEqual(scope.best_provider('A', []), close_provider)

  def test_best_provider_based_on_multiple_followers(self):
    close_provider = TestProvider('A', 'xxy')
    provider_B = TestProvider('B', 'xxx')
    # These are not quite as close as the two above.
    far_provider = TestProvider('A', 'yyy')
    provider_C = TestProvider('C', 'yzz')
    scope = self.create_scope(
        '''
        type: composite
        transforms:
          - type: A
          - type: B
            input: A
          - type: C
            input: A
        ''', [far_provider, close_provider, provider_B, provider_C])
    self.assertEqual(scope.best_provider('A', []), close_provider)

  def test_best_provider_based_on_distant_follower(self):
    providers = [
        # xxx and yyy vend both
        TestProvider('A', 'xxx'),
        TestProvider('A', 'yyy'),
        TestProvider('B', 'xxx'),
        TestProvider('B', 'yyy'),
        TestProvider('C', 'xxx'),
        TestProvider('C', 'yyy'),
        # D and E are only provided by a single provider each.
        TestProvider('D', 'xxx'),
        TestProvider('E', 'yyy')
    ]

    # If D is the eventual destination, pick the xxx one.
    scope = self.create_scope(
        '''
        type: chain
        transforms:
          - type: A
          - type: B
          - type: C
          - type: D
        ''',
        providers)
    self.assertEqual(scope.best_provider('A', []), providers[0])

    # If instead E is the eventual destination, pick the yyy one.
    scope = self.create_scope(
        '''
        type: chain
        transforms:
          - type: A
          - type: B
          - type: C
          - type: E
        ''',
        providers)
    self.assertEqual(scope.best_provider('A', []), providers[1])

    # If we have D then E, stay with xxx as long as possible to only switch once
    scope = self.create_scope(
        '''
        type: chain
        transforms:
          - type: A
          - type: B
          - type: C
          - type: D
          - type: E
        ''',
        providers)
    self.assertEqual(scope.best_provider('A', []), providers[0])


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
