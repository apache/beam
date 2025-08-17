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
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform


class ToRow(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.Map(lambda row: beam.Row(**row._asdict()))


FRUITS = [
    beam.Row(id=1, name='raspberry'),
    beam.Row(id=2, name='blackberry'),
]

QUANTITIES = [
    beam.Row(name='raspberry', quantity=1),
    beam.Row(name='blackberry', quantity=2),
    beam.Row(name='blueberry', quantity=3),
]

CATEGORIES = [
    beam.Row(name='raspberry', category='juicy'),
    beam.Row(name='blackberry', category='dry'),
    beam.Row(name='blueberry', category='dry'),
    beam.Row(name='blueberry', category='juicy'),
]


@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    'Do not run this test on precommit suites.')
class YamlJoinTest(unittest.TestCase):
  def test_basic_join(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      result = {
          "fruits": fruits, "quantities": quantities
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
        config:
          type: inner
          equalities:
            - f: name
              q: name
        ''') | ToRow()
      assert_that(
          result,
          equal_to([
              beam.Row(id=1, name='raspberry', name0='raspberry', quantity=1),
              beam.Row(id=2, name='blackberry', name0='blackberry', quantity=2)
          ]))

  def test_join_three_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)
      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities:
            - f: name
              q: name
            - f: name
              c: name
        ''') | ToRow()
      assert_that(
          result,
          equal_to([
              beam.Row(
                  id=1,
                  name='raspberry',
                  name0='raspberry',
                  quantity=1,
                  name1='raspberry',
                  category='juicy'),
              beam.Row(
                  id=2,
                  name='blackberry',
                  name0='blackberry',
                  quantity=2,
                  name1='blackberry',
                  category='dry')
          ]))

  def test_join_with_fields(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)
      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities:
            - f: name
              q: name
            - f: name
              c: name
          fields:
            f:
              f_id: id
            q:
              - name
              - quantity    
        ''') | ToRow()

      assert_that(
          result,
          equal_to([
              beam.Row(
                  f_id=1,
                  name='raspberry',
                  quantity=1,
                  name0='raspberry',
                  category='juicy'),
              beam.Row(
                  f_id=2,
                  name='blackberry',
                  quantity=2,
                  name0='blackberry',
                  category='dry')
          ]))

  def test_join_with_equalities_shorthand(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)

      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities: name
        ''') | ToRow()

      assert_that(
          result,
          equal_to([
              beam.Row(
                  id=1,
                  name='raspberry',
                  name0='raspberry',
                  quantity=1,
                  name1='raspberry',
                  category='juicy'),
              beam.Row(
                  id=2,
                  name='blackberry',
                  name0='blackberry',
                  quantity=2,
                  name1='blackberry',
                  category='dry')
          ]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
