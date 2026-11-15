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
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.yaml.yaml_transform import YamlTransform

DATA = [
    beam.Row(tenant='a', event_id=1, value=10),
    beam.Row(tenant='a', event_id=1, value=10),
    beam.Row(tenant='a', event_id=2, value=20),
    beam.Row(tenant='b', event_id=1, value=30),
    beam.Row(tenant='b', event_id=1, value=30),
]

DATA_TYPE = RowTypeConstraint.from_fields([('tenant', str), ('event_id', int),
                                           ('value', int)])


def _options():
  return beam.options.pipeline_options.PipelineOptions(
      pickle_library='cloudpickle')


class YamlDeduplicateTest(unittest.TestCase):
  def test_single_key(self):
    with beam.Pipeline(options=_options()) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: event_id
            duration: 10m
          ''')
      assert_that(result | beam.Map(lambda x: x.event_id), equal_to([1, 2]))

  def test_multiple_keys(self):
    with beam.Pipeline(options=_options()) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: [tenant, event_id]
            duration: 10m
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(tenant='a', event_id=1, value=10),
              beam.Row(tenant='a', event_id=2, value=20),
              beam.Row(tenant='b', event_id=1, value=30),
          ]))

  def test_event_time(self):
    with beam.Pipeline(options=_options()) as p:
      elements = p | beam.Create(DATA)
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: [tenant, event_id]
            duration: 30s
            time_domain: event_time
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(tenant='a', event_id=1, value=10),
              beam.Row(tenant='a', event_id=2, value=20),
              beam.Row(tenant='b', event_id=1, value=30),
          ]))

  def test_nullable_key(self):
    with beam.Pipeline(options=_options()) as p:
      elements = p | beam.Create([
          beam.Row(tenant='a', event_id=1),
          beam.Row(tenant=None, event_id=1),
          beam.Row(tenant=None, event_id=1),
      ])
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: [tenant, event_id]
            duration: 10m
          ''')
      assert_that(
          result | beam.Map(lambda x: beam.Row(**x._asdict())),
          equal_to([
              beam.Row(tenant='a', event_id=1),
              beam.Row(tenant=None, event_id=1),
          ]))

  def test_preserves_schema_and_timestamps(self):
    with beam.Pipeline(options=_options()) as p:
      elements = (
          p
          | beam.Create([(row, i) for i, row in enumerate(DATA)])
          | beam.MapTuple(window.TimestampedValue).with_output_types(DATA_TYPE))
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: [tenant, event_id]
            duration: 10m
            time_domain: event_time
          ''')
      self.assertEqual(
          named_fields_from_element_type(result.element_type),
          named_fields_from_element_type(elements.element_type))
      assert_that(
          result | beam.Map(
              lambda x, ts=beam.DoFn.TimestampParam:
              (x.tenant, x.event_id, ts)),
          equal_to([('a', 1, 0), ('a', 2, 2), ('b', 1, 3)]))

  def test_different_windows(self):
    with beam.Pipeline(options=_options()) as p:
      elements = (
          p
          | beam.Create(list(zip(DATA, [0, 5, 20, 30, 45])))
          | beam.MapTuple(window.TimestampedValue).with_output_types(DATA_TYPE)
          | beam.WindowInto(window.FixedWindows(15)))
      result = elements | YamlTransform(
          '''
          type: Deduplicate
          config:
            keys: [tenant, event_id]
            duration: 10m
            time_domain: event_time
          ''')
      # The second b/1 record is in a different window from the first.
      assert_that(
          result | beam.Map(lambda x: (x.tenant, x.event_id)),
          equal_to([('a', 1), ('a', 2), ('b', 1), ('b', 1)]))

  def test_counters(self):
    p = beam.Pipeline(options=_options())
    _ = p | beam.Create(DATA) | YamlTransform(
        '''
        type: Deduplicate
        config:
          keys: [tenant, event_id]
          duration: 10m
        ''')
    result = p.run()
    result.wait_until_finish()

    def counter(name):
      counters = result.metrics().query(
          MetricsFilter().with_name(name))['counters']
      return sum(c.committed for c in counters)

    self.assertEqual(counter('deduplicate_kept'), 3)
    self.assertEqual(counter('deduplicate_dropped'), 2)

  def test_unknown_key(self):
    with self.assertRaisesRegex(ValueError, 'Unknown key fields'):
      with beam.Pipeline(options=_options()) as p:
        elements = p | beam.Create(DATA)
        _ = elements | YamlTransform(
            '''
            type: Deduplicate
            config:
              keys: [tenant, missing]
              duration: 10m
            ''')

  def test_unknown_time_domain(self):
    with self.assertRaisesRegex(ValueError, 'Unknown time_domain'):
      with beam.Pipeline(options=_options()) as p:
        elements = p | beam.Create(DATA)
        _ = elements | YamlTransform(
            '''
            type: Deduplicate
            config:
              keys: [tenant]
              duration: 10m
              time_domain: wall_clock
            ''')

  def test_invalid_duration(self):
    with self.assertRaisesRegex(ValueError, 'Invalid windowing duration'):
      with beam.Pipeline(options=_options()) as p:
        elements = p | beam.Create(DATA)
        _ = elements | YamlTransform(
            '''
            type: Deduplicate
            config:
              keys: [tenant]
              duration: 10x
            ''')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
