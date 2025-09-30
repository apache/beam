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
import typing
import unittest

import numpy as np

import apache_beam as beam
from apache_beam import schema_pb2
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import schemas
from apache_beam.utils.timestamp import Timestamp
from apache_beam.yaml import yaml_mapping
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

  def test_validate(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(key='good', small=[5], nested=beam.Row(big=100)),
          beam.Row(key='bad1', small=[500], nested=beam.Row(big=100)),
          beam.Row(key='bad2', small=[5], nested=beam.Row(big=1)),
      ])
      result = elements | YamlTransform(
          '''
          type: ValidateWithSchema
          config:
            schema:
              type: object
              properties:
                small:
                  type: array
                  items:
                    type: integer
                    maximum: 10
                nested:
                  type: object
                  properties:
                    big:
                      type: integer
                      minimum: 10
            error_handling:
              output: bad
          ''')

      assert_that(
          result['good'] | beam.Map(lambda x: x.key), equal_to(['good']))
      assert_that(
          result['bad'] | beam.Map(lambda x: x.element.key),
          equal_to(['bad1', 'bad2']),
          label='Errors')

  def test_validate_explicit_types(self):
    with self.assertRaisesRegex(Exception, r'.*violates schema.*'):
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

  def test_partition(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple'),
          beam.Row(element='banana'),
          beam.Row(element='orange'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by: "'even' if len(element) % 2 == 0 else 'odd'"
            language: python
            outputs: [even, odd]
          ''')
      self.assertEqual(result['even'].element_type, elements.element_type)
      assert_that(
          result['even'] | beam.Map(lambda x: x.element),
          equal_to(['banana', 'orange']),
          label='Even')
      assert_that(
          result['odd'] | beam.Map(lambda x: x.element),
          equal_to(['apple']),
          label='Odd')

  def test_partition_callable(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple'),
          beam.Row(element='banana'),
          beam.Row(element='orange'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by:
              callable:
                "lambda row: 'even' if len(row.element) % 2 == 0 else 'odd'"
            language: python
            outputs: [even, odd]
          ''')
      assert_that(
          result['even'] | beam.Map(lambda x: x.element),
          equal_to(['banana', 'orange']),
          label='Even')
      assert_that(
          result['odd'] | beam.Map(lambda x: x.element),
          equal_to(['apple']),
          label='Odd')

  def test_partition_with_unknown(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple'),
          beam.Row(element='banana'),
          beam.Row(element='orange'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by: "element.lower()[0]"
            language: python
            outputs: [a, b, c]
            unknown_output: other
          ''')
      assert_that(
          result['a'] | beam.Map(lambda x: x.element),
          equal_to(['apple']),
          label='A')
      assert_that(
          result['b'] | beam.Map(lambda x: x.element),
          equal_to(['banana']),
          label='B')
      assert_that(
          result['c'] | beam.Map(lambda x: x.element), equal_to([]), label='C')
      assert_that(
          result['other'] | beam.Map(lambda x: x.element),
          equal_to(['orange']),
          label='Other')

  def test_partition_without_unknown(self):
    with self.assertRaisesRegex(Exception, r'.*Unknown output name.*"o".*'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create([
            beam.Row(element='apple'),
            beam.Row(element='banana'),
            beam.Row(element='orange'),
        ])
        _ = elements | YamlTransform(
            '''
            type: Partition
            input: input
            config:
              by: "element.lower()[0]"
              language: python
              outputs: [a, b, c]
            ''')

  def test_partition_without_unknown_with_error(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple'),
          beam.Row(element='banana'),
          beam.Row(element='orange'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by: "element.lower()[0]"
            language: python
            outputs: [a, b, c]
            error_handling:
              output: unknown
          ''')
      assert_that(
          result['a'] | beam.Map(lambda x: x.element),
          equal_to(['apple']),
          label='A')
      assert_that(
          result['b'] | beam.Map(lambda x: x.element),
          equal_to(['banana']),
          label='B')
      assert_that(
          result['c'] | beam.Map(lambda x: x.element), equal_to([]), label='C')
      assert_that(
          result['unknown'] | beam.Map(lambda x: x.element.element),
          equal_to(['orange']),
          label='Errors')

  def test_partition_with_actual_error(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple'),
          beam.Row(element='banana'),
          beam.Row(element='orange'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by: "element.lower()[5]"
            language: python
            outputs: [a, b, c]
            unknown_output: other
            error_handling:
              output: errors
          ''')
      assert_that(
          result['a'] | beam.Map(lambda x: x.element),
          equal_to(['banana']),
          label='B')
      assert_that(
          result['other'] | beam.Map(lambda x: x.element),
          equal_to(['orange']),
          label='Other')
      # Apple only has 5 letters, resulting in an index error.
      assert_that(
          result['errors'] | beam.Map(lambda x: x.element.element),
          equal_to(['apple']),
          label='Errors')

  def test_partition_no_language(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([
          beam.Row(element='apple', texture='smooth'),
          beam.Row(element='banana', texture='smooth'),
          beam.Row(element='orange', texture='bumpy'),
      ])
      result = elements | YamlTransform(
          '''
          type: Partition
          input: input
          config:
            by: texture
            outputs: [bumpy, smooth]
          ''')
      assert_that(
          result['bumpy'] | beam.Map(lambda x: x.element),
          equal_to(['orange']),
          label='Bumpy')
      assert_that(
          result['smooth'] | beam.Map(lambda x: x.element),
          equal_to(['apple', 'banana']),
          label='Smooth')

  def test_partition_bad_static_type(self):
    with self.assertRaisesRegex(
        ValueError, r'.*Partition function .*must return a string.*'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create([
            beam.Row(element='apple', texture='smooth'),
            beam.Row(element='banana', texture='smooth'),
            beam.Row(element='orange', texture='bumpy'),
        ])
        _ = elements | YamlTransform(
            '''
            type: Partition
            input: input
            config:
              by: len(texture)
              outputs: [bumpy, smooth]
              language: python
            ''')

  def test_partition_bad_runtime_type(self):
    with self.assertRaisesRegex(Exception,
                                r'Returned output name.*must be a string.*'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        elements = p | beam.Create([
            beam.Row(element='apple', texture='smooth'),
            beam.Row(element='banana', texture='smooth'),
            beam.Row(element='orange', texture='bumpy'),
        ])
        _ = elements | YamlTransform(
            '''
            type: Partition
            input: input
            config:
              by: print(texture)
              outputs: [bumpy, smooth]
              language: python
            ''')

  def test_append_type_inference(self):
    p = beam.Pipeline(
        options=beam.options.pipeline_options.PipelineOptions(
            pickle_library='cloudpickle'))
    elements = p | beam.Create(DATA)
    elements.element_type = schemas.named_tuple_from_schema(
        schema_pb2.Schema(
            fields=[
                schemas.schema_field('label', str),
                schemas.schema_field('conductor', int),
                schemas.schema_field('rank', int)
            ]))
    result = elements | YamlTransform(
        '''
        type: MapToFields
        config:
            language: python
            append: true
            fields:
              new_label: label
        ''')
    self.assertSequenceEqual(
        result.element_type._fields,
        (('label', str), ('conductor', np.int64), ('rank', np.int64),
         ('new_label', str)))

  def test_extract_windowing_info(self):
    T = typing.TypeVar('T')
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = (
          p
          | beam.Create(
              [beam.Row(value=1), beam.Row(value=2), beam.Row(value=11)])
          | beam.Map(
              lambda x: beam.transforms.window.TimestampedValue(
                  x, timestamp=x.value)).with_input_types(T).with_output_types(
                      T)
          | beam.WindowInto(beam.transforms.window.FixedWindows(10)))
      result = elements | YamlTransform(
          '''
          type: ExtractWindowingInfo
          config:
              fields:
                timestamp: timestamp
                window_start: window_start
                window_end: window_end
                window_string: window_string
                window_type: window_type
                window_object: window_object
                pane_info_field: pane_info
          ''')
      assert_that(
          result,
          equal_to([
              beam.Row(
                  value=1,
                  timestamp=Timestamp(1),
                  window_start=Timestamp(0),
                  window_end=Timestamp(10),
                  window_string='[0.0, 10.0)',
                  window_type='IntervalWindow',
                  window_object=beam.transforms.window.IntervalWindow(0, 10),
                  pane_info_field=yaml_mapping.PaneInfoTuple(
                      True, True, 'UNKNOWN', 0, 0)),
              beam.Row(
                  value=2,
                  timestamp=Timestamp(2),
                  window_start=Timestamp(0),
                  window_end=Timestamp(10),
                  window_string='[0.0, 10.0)',
                  window_type='IntervalWindow',
                  window_object=beam.transforms.window.IntervalWindow(0, 10),
                  pane_info_field=yaml_mapping.PaneInfoTuple(
                      True, True, 'UNKNOWN', 0, 0)),
              beam.Row(
                  value=11,
                  timestamp=Timestamp(11),
                  window_start=Timestamp(10),
                  window_end=Timestamp(20),
                  window_string='[10.0, 20.0)',
                  window_type='IntervalWindow',
                  window_object=beam.transforms.window.IntervalWindow(10, 20),
                  pane_info_field=yaml_mapping.PaneInfoTuple(
                      True, True, 'UNKNOWN', 0, 0)),
          ]))

  def test_extract_windowing_info_iterable(self):
    T = typing.TypeVar('T')
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = (
          p
          | beam.Create(
              [beam.Row(value=1), beam.Row(value=2), beam.Row(value=11)])
          | beam.Map(
              lambda x: beam.transforms.window.TimestampedValue(
                  x, timestamp=x.value)).with_input_types(T).with_output_types(
                      T))
      result = elements | YamlTransform(
          '''
          type: ExtractWindowingInfo
          config:
              fields: [timestamp, window_type]
          ''')
      assert_that(
          result,
          equal_to([
              beam.Row(
                  value=1, timestamp=Timestamp(1), window_type='GlobalWindow'),
              beam.Row(
                  value=2, timestamp=Timestamp(2), window_type='GlobalWindow'),
              beam.Row(
                  value=11, timestamp=Timestamp(11),
                  window_type='GlobalWindow'),
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
