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

"""Unit tests for the test_stream module."""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import ElementEvent
from apache_beam.testing.test_stream import ProcessingTimeEvent
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue


class TestStreamTest(unittest.TestCase):

  def test_basic_test_stream(self):
    test_stream = (TestStream()
                   .advance_watermark_to(0)
                   .add_elements([
                       'a',
                       WindowedValue('b', 3, []),
                       TimestampedValue('c', 6)])
                   .advance_processing_time(10)
                   .advance_watermark_to(8)
                   .add_elements(['d'])
                   .advance_watermark_to_infinity())
    self.assertEqual(
        test_stream.events,
        [
            WatermarkEvent(0),
            ElementEvent([
                TimestampedValue('a', 0),
                TimestampedValue('b', 3),
                TimestampedValue('c', 6),
            ]),
            ProcessingTimeEvent(10),
            WatermarkEvent(8),
            ElementEvent([
                TimestampedValue('d', 8),
            ]),
            WatermarkEvent(timestamp.MAX_TIMESTAMP),
        ]
    )

  def test_test_stream_errors(self):
    with self.assertRaises(AssertionError, msg=(
        'Watermark must strictly-monotonically advance.')):
      _ = (TestStream()
           .advance_watermark_to(5)
           .advance_watermark_to(4))

    with self.assertRaises(AssertionError, msg=(
        'Must advance processing time by positive amount.')):
      _ = (TestStream()
           .advance_processing_time(-1))

    with self.assertRaises(AssertionError, msg=(
        'Element timestamp must be before timestamp.MAX_TIMESTAMP.')):
      _ = (TestStream()
           .add_elements([
               TimestampedValue('a', timestamp.MAX_TIMESTAMP)
           ]))

  def test_basic_execution(self):
    test_stream = (TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['a', 'b', 'c'])
                   .advance_watermark_to(20)
                   .add_elements(['d'])
                   .add_elements(['e'])
                   .advance_processing_time(10)
                   .advance_watermark_to(300)
                   .add_elements([TimestampedValue('late', 12)])
                   .add_elements([TimestampedValue('last', 310)]))

    class RecordFn(beam.DoFn):
      def process(self, element=beam.DoFn.ElementParam,
                  timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    my_record_fn = RecordFn()
    records = p | test_stream | beam.ParDo(my_record_fn)

    assert_that(records, equal_to([
        ('a', timestamp.Timestamp(10)),
        ('b', timestamp.Timestamp(10)),
        ('c', timestamp.Timestamp(10)),
        ('d', timestamp.Timestamp(20)),
        ('e', timestamp.Timestamp(20)),
        ('late', timestamp.Timestamp(12)),
        ('last', timestamp.Timestamp(310)),]))

    p.run()

  def test_gbk_execution_no_triggers(self):
    test_stream = (TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['a', 'b', 'c'])
                   .advance_watermark_to(20)
                   .add_elements(['d'])
                   .add_elements(['e'])
                   .advance_processing_time(10)
                   .advance_watermark_to(300)
                   .add_elements([TimestampedValue('late', 12)])
                   .add_elements([TimestampedValue('last', 310)]))

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    records = (p
               | test_stream
               | beam.WindowInto(FixedWindows(15))
               | beam.Map(lambda x: ('k', x))
               | beam.GroupByKey())

    # TODO(BEAM-2519): timestamp assignment for elements from a GBK should
    # respect the TimestampCombiner.  The test below should also verify the
    # timestamps of the outputted elements once this is implemented.

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(0, 15): [
            ('k', ['a', 'b', 'c']),
            ('k', ['late']),
        ],
        window.IntervalWindow(15, 30): [
            ('k', ['d', 'e']),
        ],
        window.IntervalWindow(300, 315): [
            ('k', ['last']),
        ],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='assert per window')

    p.run()

  def test_gbk_execution_after_watermark_trigger(self):
    test_stream = (TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['a'])
                   .advance_watermark_to(20))

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    records = (p            # pylint: disable=unused-variable
               | test_stream
               | beam.WindowInto(
                   FixedWindows(15),
                   trigger=trigger.AfterWatermark(early=trigger.AfterCount(1)),
                   accumulation_mode=trigger.AccumulationMode.DISCARDING)
               | beam.Map(lambda x: ('k', x))
               | beam.GroupByKey())

    # TODO(BEAM-2519): timestamp assignment for elements from a GBK should
    # respect the TimestampCombiner.  The test below should also verify the
    # timestamps of the outputted elements once this is implemented.

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(15, 30): [
            ('k', ['a']),
            ('k', []),
        ],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='assert per window')

    p.run()

  def test_gbk_execution_after_processing_trigger_fired(self):
    """Advance TestClock to (X + delta) and see the pipeline does finish."""
    # TODO(mariagh): Add test_gbk_execution_after_processing_trigger_unfired
    # Advance TestClock to (X + delta) and see the pipeline does finish
    # Possibly to the framework trigger_transcripts.yaml

    test_stream = (TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['a'])
                   .advance_processing_time(5.1))

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    records = (p
               | test_stream
               | beam.WindowInto(
                   beam.window.FixedWindows(15),
                   trigger=trigger.AfterProcessingTime(5),
                   accumulation_mode=trigger.AccumulationMode.DISCARDING
                   )
               | beam.Map(lambda x: ('k', x))
               | beam.GroupByKey())

    # TODO(BEAM-2519): timestamp assignment for elements from a GBK should
    # respect the TimestampCombiner.  The test below should also verify the
    # timestamps of the outputted elements once this is implemented.

    expected_window_to_elements = {
        window.IntervalWindow(0, 15): [('k', ['a'])],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='assert per window')

    p.run()

  def test_basic_execution_batch_sideinputs(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (p
                   | 'main TestStream' >> TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['e']))
    side = (p
            | beam.Create([2, 1, 4])
            | beam.Map(lambda t: window.TimestampedValue(t, t)))

    class RecordFn(beam.DoFn):
      def process(self,
                  elm=beam.DoFn.ElementParam,
                  ts=beam.DoFn.TimestampParam,
                  side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (main_stream     # pylint: disable=unused-variable
               | beam.ParDo(RecordFn(), beam.pvalue.AsList(side)))

    assert_that(records, equal_to([('e', Timestamp(10), [2, 1, 4])]))

    p.run()

  def test_basic_execution_sideinputs(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (p
                   | 'main TestStream' >> TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['e']))
    side_stream = (p
                   | 'side TestStream' >> TestStream()
                   .add_elements([window.TimestampedValue(2, 2)])
                   .add_elements([window.TimestampedValue(1, 1)])
                   .add_elements([window.TimestampedValue(7, 7)])
                   .add_elements([window.TimestampedValue(4, 4)])
                  )

    class RecordFn(beam.DoFn):
      def process(self,
                  elm=beam.DoFn.ElementParam,
                  ts=beam.DoFn.TimestampParam,
                  side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (main_stream        # pylint: disable=unused-variable
               | beam.ParDo(RecordFn(), beam.pvalue.AsList(side_stream)))

    assert_that(records, equal_to([('e', Timestamp(10), [2, 1, 7, 4])]))

    p.run()

  def test_basic_execution_batch_sideinputs_fixed_windows(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (p
                   | 'main TestStream' >> TestStream()
                   .advance_watermark_to(2)
                   .add_elements(['a'])
                   .advance_watermark_to(4)
                   .add_elements(['b'])
                   | 'main window' >> beam.WindowInto(window.FixedWindows(1)))
    side = (p
            | beam.Create([2, 1, 4])
            | beam.Map(lambda t: window.TimestampedValue(t, t))
            | beam.WindowInto(window.FixedWindows(2)))

    class RecordFn(beam.DoFn):
      def process(self,
                  elm=beam.DoFn.ElementParam,
                  ts=beam.DoFn.TimestampParam,
                  side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (main_stream     # pylint: disable=unused-variable
               | beam.ParDo(RecordFn(), beam.pvalue.AsList(side)))

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(2, 3):[('a', Timestamp(2), [2])],
        window.IntervalWindow(4, 5):[('b', Timestamp(4), [4])]
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='assert per window')

    p.run()

  def test_basic_execution_sideinputs_fixed_windows(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (p
                   | 'main TestStream' >> TestStream()
                   .advance_watermark_to(9)
                   .add_elements(['a1', 'a2', 'a3', 'a4'])
                   .add_elements(['b'])
                   .advance_watermark_to(18)
                   .add_elements('c')
                   | 'main windowInto' >> beam.WindowInto(
                       window.FixedWindows(1))
                  )
    side_stream = (p
                   | 'side TestStream' >> TestStream()
                   .advance_watermark_to(12)
                   .add_elements([window.TimestampedValue('s1', 10)])
                   .advance_watermark_to(20)
                   .add_elements([window.TimestampedValue('s2', 20)])
                   | 'side windowInto' >> beam.WindowInto(
                       window.FixedWindows(3))
                  )

    class RecordFn(beam.DoFn):
      def process(self,
                  elm=beam.DoFn.ElementParam,
                  ts=beam.DoFn.TimestampParam,
                  side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (main_stream     # pylint: disable=unused-variable
               | beam.ParDo(RecordFn(), beam.pvalue.AsList(side_stream)))

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(9, 10): [
            ('a1', Timestamp(9), ['s1']),
            ('a2', Timestamp(9), ['s1']),
            ('a3', Timestamp(9), ['s1']),
            ('a4', Timestamp(9), ['s1']),
            ('b', Timestamp(9), ['s1'])
        ],
        window.IntervalWindow(18, 19):[('c', Timestamp(18), ['s2'])],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        use_global_window=False,
        label='assert per window')

    p.run()


if __name__ == '__main__':
  unittest.main()
