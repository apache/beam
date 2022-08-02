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

"""Integration tests for the test_stream module."""

# pytype: skip-file

import unittest
from functools import wraps

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Timestamp


def supported(runners):
  if not isinstance(runners, list):
    runners = [runners]

  def inner(fn):
    @wraps(fn)
    def wrapped(self):
      if self.runner_name not in runners:
        self.skipTest(
            'The "{}", does not support the TestStream transform. '
            'Supported runners: {}'.format(self.runner_name, runners))
      else:
        return fn(self)

    return wrapped

  return inner


class TestStreamIntegrationTests(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.test_pipeline = TestPipeline(is_integration_test=True)
    cls.args = cls.test_pipeline.get_full_options_as_args()
    cls.runner_name = type(cls.test_pipeline.runner).__name__
    cls.project = cls.test_pipeline.get_option('project')

  @supported(['DirectRunner', 'SwitchingDirectRunner'])
  @pytest.mark.it_postcommit
  def test_basic_execution(self):
    test_stream = (
        TestStream().advance_watermark_to(10).add_elements([
            'a', 'b', 'c'
        ]).advance_watermark_to(20).add_elements(['d']).add_elements([
            'e'
        ]).advance_processing_time(10).advance_watermark_to(300).add_elements([
            TimestampedValue('late', 12)
        ]).add_elements([TimestampedValue('last', 310)
                         ]).advance_watermark_to_infinity())

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    with beam.Pipeline(argv=self.args) as p:
      my_record_fn = RecordFn()
      records = p | test_stream | beam.ParDo(my_record_fn)

      assert_that(
          records,
          equal_to([
              ('a', timestamp.Timestamp(10)),
              ('b', timestamp.Timestamp(10)),
              ('c', timestamp.Timestamp(10)),
              ('d', timestamp.Timestamp(20)),
              ('e', timestamp.Timestamp(20)),
              ('late', timestamp.Timestamp(12)),
              ('last', timestamp.Timestamp(310)),
          ]))

  @supported(['DirectRunner', 'SwitchingDirectRunner'])
  @pytest.mark.it_postcommit
  def test_multiple_outputs(self):
    """Tests that the TestStream supports emitting to multiple PCollections."""
    letters_elements = [
        TimestampedValue('a', 6),
        TimestampedValue('b', 7),
        TimestampedValue('c', 8),
    ]
    numbers_elements = [
        TimestampedValue('1', 11),
        TimestampedValue('2', 12),
        TimestampedValue('3', 13),
    ]
    test_stream = (
        TestStream().advance_watermark_to(5, tag='letters').add_elements(
            letters_elements,
            tag='letters').advance_watermark_to(10, tag='numbers').add_elements(
                numbers_elements, tag='numbers'))

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = StandardOptions(streaming=True)
    p = TestPipeline(is_integration_test=True, options=options)

    main = p | test_stream
    letters = main['letters'] | 'record letters' >> beam.ParDo(RecordFn())
    numbers = main['numbers'] | 'record numbers' >> beam.ParDo(RecordFn())

    assert_that(
        letters,
        equal_to([('a', Timestamp(6)), ('b', Timestamp(7)),
                  ('c', Timestamp(8))]),
        label='assert letters')

    assert_that(
        numbers,
        equal_to([('1', Timestamp(11)), ('2', Timestamp(12)),
                  ('3', Timestamp(13))]),
        label='assert numbers')

    p.run()

  @supported(['DirectRunner', 'SwitchingDirectRunner'])
  @pytest.mark.it_postcommit
  def test_multiple_outputs_with_watermark_advancement(self):
    """Tests that the TestStream can independently control output watermarks."""

    # Purposely set the watermark of numbers to 20 then letters to 5 to test
    # that the watermark advancement is per PCollection.
    #
    # This creates two PCollections, (a, b, c) and (1, 2, 3). These will be
    # emitted at different times so that they will have different windows. The
    # watermark advancement is checked by checking their windows. If the
    # watermark does not advance, then the windows will be [-inf, -inf). If the
    # windows do not advance separately, then the PCollections will both
    # windowed in [15, 30).
    letters_elements = [
        TimestampedValue('a', 6),
        TimestampedValue('b', 7),
        TimestampedValue('c', 8),
    ]
    numbers_elements = [
        TimestampedValue('1', 21),
        TimestampedValue('2', 22),
        TimestampedValue('3', 23),
    ]
    test_stream = (
        TestStream().advance_watermark_to(
            0, tag='letters').advance_watermark_to(
                0, tag='numbers').advance_watermark_to(
                    20, tag='numbers').advance_watermark_to(
                        5, tag='letters').add_elements(
                            letters_elements,
                            tag='letters').advance_watermark_to(
                                10, tag='letters').add_elements(
                                    numbers_elements,
                                    tag='numbers').advance_watermark_to(
                                        30, tag='numbers'))

    options = StandardOptions(streaming=True)
    p = TestPipeline(is_integration_test=True, options=options)

    main = p | test_stream

    # Use an AfterWatermark trigger with an early firing to test that the
    # watermark is advancing properly and that the element is being emitted in
    # the correct window.
    letters = (
        main['letters']
        | 'letter windows' >> beam.WindowInto(
            FixedWindows(15),
            trigger=trigger.AfterWatermark(early=trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'letter with key' >> beam.Map(lambda x: ('k', x))
        | 'letter gbk' >> beam.GroupByKey())

    numbers = (
        main['numbers']
        | 'number windows' >> beam.WindowInto(
            FixedWindows(15),
            trigger=trigger.AfterWatermark(early=trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'number with key' >> beam.Map(lambda x: ('k', x))
        | 'number gbk' >> beam.GroupByKey())

    # The letters were emitted when the watermark was at 5, thus we expect to
    # see the elements in the [0, 15) window. We used an early trigger to make
    # sure that the ON_TIME empty pane was also emitted with a TestStream.
    # This pane has no data because of the early trigger causes the elements to
    # fire before the end of the window and because the accumulation mode
    # discards any data after the trigger fired.
    expected_letters = {
        window.IntervalWindow(0, 15): [
            ('k', ['a', 'b', 'c']),
            ('k', []),
        ],
    }

    # Same here, except the numbers were emitted at watermark = 20, thus they
    # are in the [15, 30) window.
    expected_numbers = {
        window.IntervalWindow(15, 30): [
            ('k', ['1', '2', '3']),
            ('k', []),
        ],
    }
    assert_that(
        letters,
        equal_to_per_window(expected_letters),
        label='letters assert per window')
    assert_that(
        numbers,
        equal_to_per_window(expected_numbers),
        label='numbers assert per window')

    p.run()


if __name__ == '__main__':
  unittest.main()
