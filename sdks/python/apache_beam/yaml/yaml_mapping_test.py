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
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.readme_test import createTestSuite
from apache_beam.yaml.yaml_transform import YamlTransform

DATA = [
    beam.Row(label='11a', conductor=11, rank=0),
    beam.Row(label='37a', conductor=37, rank=1),
    beam.Row(label='389a', conductor=389, rank=2),
]


class YamlMappingTest(unittest.TestCase):
  def test_basic(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: MapToFields
          config:
              language: python
              fields:
                label: label
                isogeny: "label[-1]"
          ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11a', isogeny='a'),
              beam.Row(label='37a', isogeny='a'),
              beam.Row(label='389a', isogeny='a'),
          ]))

  def test_drop(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: MapToFields
          config:
              fields: {}
              append: true
              drop: [conductor]
          ''')
      assert_that(
          result,
          equal_to([
              beam.Row(label='11a', rank=0),
              beam.Row(label='37a', rank=1),
              beam.Row(label='389a', rank=2),
          ]))

  def test_filter(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Filter
          config:
              language: python
              keep: "rank > 0"
          ''')
      assert_that(
          result
          | beam.Map(lambda named_tuple: beam.Row(**named_tuple._asdict())),
          equal_to([
              beam.Row(label='37a', conductor=37, rank=1),
              beam.Row(label='389a', conductor=389, rank=2),
          ]))

  def test_explode(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(a=2, b='abc', c=.25),
          beam.Row(a=3, b='xy', c=.125),
      ])
      result = elements | YamlTransform(
          '''
          type: chain
          transforms:
            - type: MapToFields
              config:
                  language: python
                  append: true
                  fields:
                    range: "range(a)"
            - type: Explode
              config:
                  fields: [range, b]
                  cross_product: true
          ''')
      assert_that(
          result,
          equal_to([
              beam.Row(a=2, b='a', c=.25, range=0),
              beam.Row(a=2, b='a', c=.25, range=1),
              beam.Row(a=2, b='b', c=.25, range=0),
              beam.Row(a=2, b='b', c=.25, range=1),
              beam.Row(a=2, b='c', c=.25, range=0),
              beam.Row(a=2, b='c', c=.25, range=1),
              beam.Row(a=3, b='x', c=.125, range=0),
              beam.Row(a=3, b='x', c=.125, range=1),
              beam.Row(a=3, b='x', c=.125, range=2),
              beam.Row(a=3, b='y', c=.125, range=0),
              beam.Row(a=3, b='y', c=.125, range=1),
              beam.Row(a=3, b='y', c=.125, range=2),
          ]))

  def test_validate_explicit_types(self):
    with self.assertRaisesRegex(TypeError, r'.*violates schema.*'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create([
            beam.Row(a=2, b='abc', c=.25),
            beam.Row(a=3, b='xy', c=.125),
        ])
        result = elements | YamlTransform(
            '''
            type: MapToFields
            input: input
            config:
              language: python
              fields:
                bad:
                  expression: "a + c"
                  output_type: string  # This is a lie.
            ''')
        self.assertEqual(result.element_type._fields[0][1], str)


YamlMappingDocTest = createTestSuite(
    'YamlMappingDocTest',
    os.path.join(os.path.dirname(__file__), 'yaml_mapping.md'))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
