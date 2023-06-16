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
import glob
import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_transform import YamlTransform


class YamlTransformTest(unittest.TestCase):
  def test_composite(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([1, 2, 3])
      # TODO(robertwb): Consider making the input implicit (and below).
      result = elements | YamlTransform(
          '''
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
            - type: Flatten
              input: [Square, Cube]
          output:
              Flatten
          ''')
      assert_that(result, equal_to([1, 4, 9, 1, 8, 27]))

  def test_chain_with_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(range(10))
      result = elements | YamlTransform(
          '''
          type: chain
          input:
              elements: input
          transforms:
            - type: PyMap
              fn: "lambda x: x * x + x"
            - type: PyMap
              fn: "lambda x: x + 41"
          ''')
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_source_sink(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          source:
            type: Create
            elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
          transforms:
            - type: PyMap
              fn: "lambda x: x * x + x"
          sink:
            type: PyMap
            fn: "lambda x: x + 41"
          ''')
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_root(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            - type: PyMap
              fn: "lambda x: x * x + x"
            - type: PyMap
              fn: "lambda x: x + 41"
          ''')
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_implicit_flatten(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              name: CreateSmall
              elements: [1, 2, 3]
            - type: Create
              name: CreateBig
              elements: [100, 200]
            - type: PyMap
              input: [CreateBig, CreateSmall]
              fn: "lambda x: x * x"
          output: PyMap
          ''')
      assert_that(result, equal_to([1, 4, 9, 10000, 40000]))

  def test_csv_to_json(self):
    try:
      import pandas as pd
    except ImportError:
      raise unittest.SkipTest('Pandas not available.')

    with tempfile.TemporaryDirectory() as tmpdir:
      data = pd.DataFrame([
          {
              'label': '11a', 'rank': 0
          },
          {
              'label': '37a', 'rank': 1
          },
          {
              'label': '389a', 'rank': 2
          },
      ])
      input = os.path.join(tmpdir, 'input.csv')
      output = os.path.join(tmpdir, 'output.json')
      data.to_csv(input, index=False)

      with beam.Pipeline() as p:
        result = p | YamlTransform(
            '''
            type: chain
            transforms:
              - type: ReadFromCsv
                path: %s
              - type: WriteToJson
                path: %s
                num_shards: 1
            ''' % (repr(input), repr(output)))

      output_shard = list(glob.glob(output + "*"))[0]
      result = pd.read_json(
          output_shard, orient='records',
          lines=True).sort_values('rank').reindex()
      pd.testing.assert_frame_equal(data, result)

  def test_name_is_not_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create
                elements: [0, 1, 3, 4]
              - type: PyFilter
                name: Filter
                keep: "lambda elem: elem > 2"
                input: Create
            output: Filter
            ''')
      # No exception raised
      assert_that(result, equal_to([3, 4]))

  def test_name_is_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # pylint: disable=expression-not-assigned
      with self.assertRaises(ValueError):
        p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: Create
                name: CreateData
                elements: [0, 1, 3, 4]
              - type: PyFilter
                name: PyFilter
                keep: "lambda elem: elem > 2"
                input: CreateData
              - type: PyFilter
                name: AnotherFilter
                keep: "lambda elem: elem > 3"
                input: PyFilter
            output: AnotherFilter
            ''')


class CreateTimestamped(beam.PTransform):
  def __init__(self, elements):
    self._elements = elements

  def expand(self, p):
    return (
        p
        | beam.Create(self._elements)
        | beam.Map(lambda x: beam.transforms.window.TimestampedValue(x, x)))


class SumGlobally(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(sum).without_defaults()


TEST_PROVIDERS = {
    'CreateTimestamped': CreateTimestamped, 'SumGlobally': SumGlobally
}


class YamlWindowingTest(unittest.TestCase):
  def test_explicit_window_into(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: WindowInto
              windowing:
                type: fixed
                size: 4
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
              windowing:
                type: fixed
                size: 4
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_multiple_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: CreateTimestamped
              name: Create1
              elements: [0, 2, 4]
            - type: CreateTimestamped
              name: Create2
              elements: [1, 3, 5]
            - type: SumGlobally
              input: [Create1, Create2]
              windowing:
                type: fixed
                size: 4
          output: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_output(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
              windowing:
                type: fixed
                size: 4
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_outer(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
          windowing:
            type: fixed
            size: 4
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))


class AnnotatingProvider(yaml_provider.InlineProvider):
  """A provider that vends transforms that do nothing but record that this
  provider (as identified by name) was used, along with any prior history
  of the given element.
  """
  def __init__(self, name, transform_names):
    super().__init__({
        transform_name: lambda: beam.Map(lambda x: (x or ()) + (name, ))
        for transform_name in transform_names.strip().split()
    })
    self._name = name

  def __repr__(self):
    return 'AnnotatingProvider(%r)' % self._name


class AnotherAnnProvider(AnnotatingProvider):
  """A Provider that behaves exactly as AnnotatingProvider, but is not
  of the same type and so is considered "more distant" for matching purposes.
  """
  pass


class ProviderAffinityTest(unittest.TestCase):
  """These tests check that for a sequence of transforms, the "closest"
  proveders are chosen among multiple possible implementations.
  """
  provider1 = AnnotatingProvider("provider1", "P1 A B C  ")
  provider2 = AnnotatingProvider("provider2", "P2 A   C D")
  provider3 = AnotherAnnProvider("provider3", "P3 A B    ")
  provider4 = AnotherAnnProvider("provider4", "P4 A B   D")

  providers_dict = collections.defaultdict(list)
  for provider in [provider1, provider2, provider3, provider4]:
    for transform_type in provider.provided_transforms():
      providers_dict[transform_type].append(provider)

  def test_prefers_same_provider(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result1 = p | 'Yaml1' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              elements: [0]
            - type: P1
            - type: A
            - type: C
          ''',
          providers=self.providers_dict)
      assert_that(
          result1,
          equal_to([(
              # provider1 was chosen, as it is the only one vending P1
              'provider1',
              # All of the providers vend A, but since the input was produced
              # by provider1, we prefer to use that again.
              'provider1',
              # Similarly for C.
              'provider1')]),
          label='StartWith1')

      result2 = p | 'Yaml2' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              elements: [0]
            - type: P2
            - type: A
            - type: C
          ''',
          providers=self.providers_dict)
      assert_that(
          result2,
          equal_to([(
              # provider2 was necessarily chosen for P2
              'provider2',
              # Unlike above, we choose provider2 to implement A.
              'provider2',
              # Likewise for C.
              'provider2')]),
          label='StartWith2')

  def test_prefers_same_provider_class(self):
    # Like test_prefers_same_provider, but as we cannot choose the same
    # exact provider, we go with the next closest (which is of the same type)
    # over an implementation from a Provider of a different type.
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result1 = p | 'Yaml1' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              elements: [0]
            - type: P1
            - type: A
            - type: D
            - type: A
          ''',
          providers=self.providers_dict)
      assert_that(
          result1,
          equal_to([('provider1', 'provider1', 'provider2', 'provider2')]),
          label='StartWith1')

      result3 = p | 'Yaml2' >> YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              elements: [0]
            - type: P3
            - type: A
            - type: D
            - type: A
          ''',
          providers=self.providers_dict)
      assert_that(
          result3,
          equal_to([('provider3', 'provider3', 'provider4', 'provider4')]),
          label='StartWith3')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
