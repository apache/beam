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

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform

DATA = [
    beam.Row(a='x', b=1, c=101),
    beam.Row(a='x', b=1, c=102),
    beam.Row(a='y', b=1, c=103),
    beam.Row(a='y', b=2, c=104),
]


class YamlCombineTest(unittest.TestCase):
  def test_multiple_aggregations(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            group_by: a
            combine:
              b: sum
              c: max
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(a='x', b=2, c=102),
              beam.Row(a='y', b=3, c=104),
          ]))

  def test_multiple_keys(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            group_by: [a, b]
            combine:
              c: sum
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(a='x', b=1, c=203),
              beam.Row(a='y', b=1, c=103),
              beam.Row(a='y', b=2, c=104),
          ]))

  def test_no_keys(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            group_by: []
            combine:
              c: sum
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(c=410),
          ]))

  def test_multiple_combines(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            group_by: a
            combine:
              min_c:
                fn: min
                value: c
              max_c:
                fn: max
                value: c
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(a='x', min_c=101, max_c=102),
              beam.Row(a='y', min_c=103, max_c=104),
          ]))

  def test_group(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            language: python
            group_by: a
            combine:
              b:
                fn: sum
                value: b
              c:
                fn: group
                value: c
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(a='x', b=2, c=[101, 102]),
              beam.Row(a='y', b=3, c=[103, 104]),
          ]))

  def test_expression(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            language: python
            group_by: a
            combine:
              max:
                fn: max
                value: b + c
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(a='x', max=103),
              beam.Row(a='y', max=106),
          ]))

  def test_config(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Combine
          config:
            language: python
            group_by: b
            combine:
              biggest:
                fn:
                  type: 'apache_beam.transforms.combiners.TopCombineFn'
                  config:
                    n: 2
                value: c
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(b=1, biggest=[103, 102]),
              beam.Row(b=2, biggest=[104]),
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
