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

# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import ElementEvent
from apache_beam.testing.test_stream import OutputFormat
from apache_beam.testing.test_stream import ProcessingTimeEvent
from apache_beam.testing.test_stream import ReverseTestStream
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.testing.test_stream import WindowedValueHolder
from apache_beam.testing.test_stream_service import TestStreamServiceController
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfo
from apache_beam.utils.windowed_value import PaneInfoTiming
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
                   .advance_watermark_to_infinity())  # yapf: disable
    self.assertEqual(
        test_stream._events,
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
        ])

  def test_test_stream_errors(self):
    with self.assertRaises(
        AssertionError, msg=('Watermark must strictly-monotonically advance.')):
      _ = (TestStream().advance_watermark_to(5).advance_watermark_to(4))

    with self.assertRaises(
        AssertionError,
        msg=('Must advance processing time by positive amount.')):
      _ = (TestStream().advance_processing_time(-1))

    with self.assertRaises(
        AssertionError,
        msg=('Element timestamp must be before timestamp.MAX_TIMESTAMP.')):
      _ = (
          TestStream().add_elements(
              [TimestampedValue('a', timestamp.MAX_TIMESTAMP)]))

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
                   .add_elements([TimestampedValue('last', 310)])
                   .advance_watermark_to_infinity())  # yapf: disable

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
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
    test_stream = (TestStream()
        .advance_watermark_to(5, tag='letters')
        .add_elements(letters_elements, tag='letters')
        .advance_watermark_to(10, tag='numbers')
        .add_elements(numbers_elements, tag='numbers'))  # yapf: disable

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = StandardOptions(streaming=True)
    p = TestPipeline(options=options)

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
    test_stream = (TestStream()
                   .advance_watermark_to(0, tag='letters')
                   .advance_watermark_to(0, tag='numbers')
                   .advance_watermark_to(20, tag='numbers')
                   .advance_watermark_to(5, tag='letters')
                   .add_elements(letters_elements, tag='letters')
                   .advance_watermark_to(10, tag='letters')
                   .add_elements(numbers_elements, tag='numbers')
                   .advance_watermark_to(30, tag='numbers')) # yapf: disable

    options = StandardOptions(streaming=True)
    p = TestPipeline(options=options)

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

  def test_dicts_not_interpreted_as_windowed_values(self):
    test_stream = (TestStream()
                   .advance_processing_time(10)
                   .advance_watermark_to(10)
                   .add_elements([{'a': 0, 'b': 1, 'c': 2}])
                   .advance_watermark_to_infinity())  # yapf: disable

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      my_record_fn = RecordFn()
      records = p | test_stream | beam.ParDo(my_record_fn)

      assert_that(
          records,
          equal_to([
              ({
                  'a': 0, 'b': 1, 'c': 2
              }, timestamp.Timestamp(10)),
          ]))

  def test_windowed_values_interpreted_correctly(self):
    windowed_value = WindowedValueHolder(
        WindowedValue(
            'a',
            Timestamp(5), [beam.window.IntervalWindow(5, 10)],
            PaneInfo(True, True, PaneInfoTiming.ON_TIME, 0, 0)))
    test_stream = (TestStream()
                   .advance_processing_time(10)
                   .advance_watermark_to(10)
                   .add_elements([windowed_value])
                   .advance_watermark_to_infinity())  # yapf: disable

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam,
          window=beam.DoFn.WindowParam):
        yield (element, timestamp, window)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      my_record_fn = RecordFn()
      records = p | test_stream | beam.ParDo(my_record_fn)

      assert_that(
          records,
          equal_to([
              ('a', timestamp.Timestamp(5), beam.window.IntervalWindow(5, 10)),
          ]))

  def test_instance_check_windowed_value_holder(self):
    windowed_value = WindowedValue(
        'a',
        Timestamp(5), [beam.window.IntervalWindow(5, 10)],
        PaneInfo(True, True, PaneInfoTiming.ON_TIME, 0, 0))
    self.assertTrue(
        isinstance(WindowedValueHolder(windowed_value), WindowedValueHolder))
    self.assertTrue(
        isinstance(
            beam.Row(
                windowed_value=windowed_value, urn=common_urns.coders.ROW.urn),
            WindowedValueHolder))
    self.assertFalse(
        isinstance(
            beam.Row(windowed_value=windowed_value), WindowedValueHolder))
    self.assertFalse(isinstance(windowed_value, WindowedValueHolder))
    self.assertFalse(
        isinstance(beam.Row(x=windowed_value), WindowedValueHolder))
    self.assertFalse(
        isinstance(beam.Row(windowed_value=1), WindowedValueHolder))

  def test_gbk_execution_no_triggers(self):
    test_stream = (
        TestStream().advance_watermark_to(10).add_elements([
            'a', 'b', 'c'
        ]).advance_watermark_to(20).add_elements(['d']).add_elements([
            'e'
        ]).advance_processing_time(10).advance_watermark_to(300).add_elements([
            TimestampedValue('late', 12)
        ]).add_elements([TimestampedValue('last', 310)
                         ]).advance_watermark_to_infinity())

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    records = (
        p
        | test_stream
        | beam.WindowInto(FixedWindows(15), allowed_lateness=300)
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
        label='assert per window')

    p.run()

  def test_gbk_execution_after_watermark_trigger(self):
    test_stream = (TestStream()
        .advance_watermark_to(10)
        .add_elements([TimestampedValue('a', 11)])
        .advance_watermark_to(20)
        .add_elements([TimestampedValue('b', 21)])
        .advance_watermark_to_infinity())  # yapf: disable

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    records = (
        p  # pylint: disable=unused-variable
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
        window.IntervalWindow(0, 15): [('k', ['a']), ('k', [])],
        window.IntervalWindow(15, 30): [('k', ['b']), ('k', [])],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
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
        .advance_processing_time(5.1)
        .advance_watermark_to_infinity())  # yapf: disable

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(TypeOptions).allow_unsafe_triggers = True
    p = TestPipeline(options=options)
    records = (
        p
        | test_stream
        | beam.WindowInto(
            beam.window.FixedWindows(15),
            trigger=trigger.AfterProcessingTime(5),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
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
        label='assert per window')

    p.run()

  def test_basic_execution_batch_sideinputs(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (p
                   | 'main TestStream' >> TestStream()
                   .advance_watermark_to(10)
                   .add_elements(['e'])
                   .advance_watermark_to_infinity())  # yapf: disable
    side = (
        p
        | beam.Create([2, 1, 4])
        | beam.Map(lambda t: window.TimestampedValue(t, t)))

    class RecordFn(beam.DoFn):
      def process(
          self,
          elm=beam.DoFn.ElementParam,
          ts=beam.DoFn.TimestampParam,
          side=beam.DoFn.SideInputParam):
        yield (elm, ts, sorted(side))

    records = (
        main_stream  # pylint: disable=unused-variable
        | beam.ParDo(RecordFn(), beam.pvalue.AsList(side)))

    assert_that(records, equal_to([('e', Timestamp(10), [1, 2, 4])]))

    p.run()

  def test_basic_execution_sideinputs(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:

      test_stream = (p | TestStream()
          .advance_watermark_to(0, tag='side')
          .advance_watermark_to(10, tag='main')
          .add_elements(['e'], tag='main')
          .add_elements([window.TimestampedValue(2, 2)], tag='side')
          .add_elements([window.TimestampedValue(1, 1)], tag='side')
          .add_elements([window.TimestampedValue(7, 7)], tag='side')
          .add_elements([window.TimestampedValue(4, 4)], tag='side')
          ) # yapf: disable

      main_stream = test_stream['main']
      side_stream = test_stream['side']

      class RecordFn(beam.DoFn):
        def process(
            self,
            elm=beam.DoFn.ElementParam,
            ts=beam.DoFn.TimestampParam,
            side=beam.DoFn.SideInputParam):
          yield (elm, ts, side)

      records = (
          main_stream  # pylint: disable=unused-variable
          | beam.ParDo(RecordFn(), beam.pvalue.AsList(side_stream)))

      assert_that(records, equal_to([('e', Timestamp(10), [2, 1, 7, 4])]))

  def test_basic_execution_batch_sideinputs_fixed_windows(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    main_stream = (
        p
        |
        'main TestStream' >> TestStream().advance_watermark_to(2).add_elements(
            ['a']).advance_watermark_to(4).add_elements(
                ['b']).advance_watermark_to_infinity()
        | 'main window' >> beam.WindowInto(window.FixedWindows(1)))
    side = (
        p
        | beam.Create([2, 1, 4])
        | beam.Map(lambda t: window.TimestampedValue(t, t))
        | beam.WindowInto(window.FixedWindows(2)))

    class RecordFn(beam.DoFn):
      def process(
          self,
          elm=beam.DoFn.ElementParam,
          ts=beam.DoFn.TimestampParam,
          side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (
        main_stream  # pylint: disable=unused-variable
        | beam.ParDo(RecordFn(), beam.pvalue.AsList(side)))

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(2, 3): [('a', Timestamp(2), [2])],
        window.IntervalWindow(4, 5): [('b', Timestamp(4), [4])]
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        label='assert per window')

    p.run()

  def test_basic_execution_sideinputs_fixed_windows(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)

    test_stream = (p | TestStream()
        .advance_watermark_to(12, tag='side')
        .add_elements([window.TimestampedValue('s1', 10)], tag='side')
        .advance_watermark_to(20, tag='side')
        .add_elements([window.TimestampedValue('s2', 20)], tag='side')

        .advance_watermark_to(9, tag='main')
        .add_elements(['a1', 'a2', 'a3', 'a4'], tag='main')
        .add_elements(['b'], tag='main')
        .advance_watermark_to(18, tag='main')
        .add_elements('c', tag='main')
        ) # yapf: disable

    main_stream = (
        test_stream['main']
        | 'main windowInto' >> beam.WindowInto(window.FixedWindows(1)))

    side_stream = (
        test_stream['side']
        | 'side windowInto' >> beam.WindowInto(window.FixedWindows(3)))

    class RecordFn(beam.DoFn):
      def process(
          self,
          elm=beam.DoFn.ElementParam,
          ts=beam.DoFn.TimestampParam,
          side=beam.DoFn.SideInputParam):
        yield (elm, ts, side)

    records = (
        main_stream  # pylint: disable=unused-variable
        | beam.ParDo(RecordFn(), beam.pvalue.AsList(side_stream)))

    # assert per window
    expected_window_to_elements = {
        window.IntervalWindow(9, 10): [
            ('a1', Timestamp(9), ['s1']), ('a2', Timestamp(9), ['s1']),
            ('a3', Timestamp(9), ['s1']), ('a4', Timestamp(9), ['s1']),
            ('b', Timestamp(9), ['s1'])
        ],
        window.IntervalWindow(18, 19): [('c', Timestamp(18), ['s2'])],
    }
    assert_that(
        records,
        equal_to_per_window(expected_window_to_elements),
        label='assert per window')

    p.run()

  def test_roundtrip_proto(self):
    test_stream = (TestStream()
                   .advance_processing_time(1)
                   .advance_watermark_to(2)
                   .add_elements([1, 2, 3])) # yapf: disable

    p = TestPipeline(options=StandardOptions(streaming=True))
    p | test_stream

    pipeline_proto, context = p.to_runner_api(return_context=True)

    for t in pipeline_proto.components.transforms.values():
      if t.spec.urn == common_urns.primitives.TEST_STREAM.urn:
        test_stream_proto = t

    self.assertTrue(test_stream_proto)
    roundtrip_test_stream = TestStream().from_runner_api(
        test_stream_proto, context)

    self.assertListEqual(test_stream._events, roundtrip_test_stream._events)
    self.assertSetEqual(
        test_stream.output_tags, roundtrip_test_stream.output_tags)
    self.assertEqual(test_stream.coder, roundtrip_test_stream.coder)

  def test_roundtrip_proto_multi(self):
    test_stream = (TestStream()
                   .advance_processing_time(1)
                   .advance_watermark_to(2, tag='a')
                   .advance_watermark_to(3, tag='b')
                   .add_elements([1, 2, 3], tag='a')
                   .add_elements([4, 5, 6], tag='b')) # yapf: disable

    options = StandardOptions(streaming=True)

    p = TestPipeline(options=options)
    p | test_stream

    pipeline_proto, context = p.to_runner_api(return_context=True)

    for t in pipeline_proto.components.transforms.values():
      if t.spec.urn == common_urns.primitives.TEST_STREAM.urn:
        test_stream_proto = t

    self.assertTrue(test_stream_proto)
    roundtrip_test_stream = TestStream().from_runner_api(
        test_stream_proto, context)

    self.assertListEqual(test_stream._events, roundtrip_test_stream._events)
    self.assertSetEqual(
        test_stream.output_tags, roundtrip_test_stream.output_tags)
    self.assertEqual(test_stream.coder, roundtrip_test_stream.coder)

  def test_basic_execution_with_service(self):
    """Tests that the TestStream can correctly read from an RPC service.
    """
    coder = beam.coders.FastPrimitivesCoder()

    test_stream_events = (TestStream(coder=coder)
        .advance_watermark_to(10000)
        .add_elements(['a', 'b', 'c'])
        .advance_watermark_to(20000)
        .add_elements(['d'])
        .add_elements(['e'])
        .advance_processing_time(10)
        .advance_watermark_to(300000)
        .add_elements([TimestampedValue('late', 12000)])
        .add_elements([TimestampedValue('last', 310000)])
        .advance_watermark_to_infinity())._events  # yapf: disable

    test_stream_proto_events = [
        e.to_runner_api(coder) for e in test_stream_events
    ]

    class InMemoryEventReader:
      def read_multiple(self, unused_keys):
        for e in test_stream_proto_events:
          yield e

    service = TestStreamServiceController(reader=InMemoryEventReader())
    service.start()

    test_stream = TestStream(coder=coder, endpoint=service.endpoint)

    class RecordFn(beam.DoFn):
      def process(
          self,
          element=beam.DoFn.ElementParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (element, timestamp)

    options = StandardOptions(streaming=True)

    p = TestPipeline(options=options)
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

    p.run()


class ReverseTestStreamTest(unittest.TestCase):
  def test_basic_execution(self):
    test_stream = (TestStream()
                   .advance_watermark_to(0)
                   .advance_processing_time(5)
                   .add_elements(['a', 'b', 'c'])
                   .advance_watermark_to(2)
                   .advance_processing_time(1)
                   .advance_watermark_to(4)
                   .advance_processing_time(1)
                   .advance_watermark_to(6)
                   .advance_processing_time(1)
                   .advance_watermark_to(8)
                   .advance_processing_time(1)
                   .advance_watermark_to(10)
                   .advance_processing_time(1)
                   .add_elements([TimestampedValue('1', 15),
                                  TimestampedValue('2', 15),
                                  TimestampedValue('3', 15)]))  # yapf: disable

    options = StandardOptions(streaming=True)
    p = TestPipeline(options=options)

    records = (
        p
        | test_stream
        | ReverseTestStream(sample_resolution_sec=1, output_tag=None))

    assert_that(
        records,
        equal_to_per_window({
            beam.window.GlobalWindow(): [
                [ProcessingTimeEvent(5), WatermarkEvent(0)],
                [
                    ElementEvent([
                        TimestampedValue('a', 0),
                        TimestampedValue('b', 0),
                        TimestampedValue('c', 0)
                    ])
                ],
                [ProcessingTimeEvent(1), WatermarkEvent(2000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(4000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(6000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(8000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(10000000)],
                [
                    ElementEvent([
                        TimestampedValue('1', 15),
                        TimestampedValue('2', 15),
                        TimestampedValue('3', 15)
                    ])
                ],
            ],
        }))

    p.run()

  def test_windowing(self):
    test_stream = (TestStream()
                   .advance_watermark_to(0)
                   .add_elements(['a', 'b', 'c'])
                   .advance_processing_time(1)
                   .advance_processing_time(1)
                   .advance_processing_time(1)
                   .advance_processing_time(1)
                   .advance_processing_time(1)
                   .advance_watermark_to(5)
                   .add_elements(['1', '2', '3'])
                   .advance_processing_time(1)
                   .advance_watermark_to(6)
                   .advance_processing_time(1)
                   .advance_watermark_to(7)
                   .advance_processing_time(1)
                   .advance_watermark_to(8)
                   .advance_processing_time(1)
                   .advance_watermark_to(9)
                   .advance_processing_time(1)
                   .advance_watermark_to(10)
                   .advance_processing_time(1)
                   .advance_watermark_to(11)
                   .advance_processing_time(1)
                   .advance_watermark_to(12)
                   .advance_processing_time(1)
                   .advance_watermark_to(13)
                   .advance_processing_time(1)
                   .advance_watermark_to(14)
                   .advance_processing_time(1)
                   .advance_watermark_to(15)
                   .advance_processing_time(1)
                   )  # yapf: disable

    options = StandardOptions(streaming=True)
    p = TestPipeline(options=options)

    records = (
        p
        | test_stream
        | 'letter windows' >> beam.WindowInto(
            FixedWindows(5),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'letter with key' >> beam.Map(lambda x: ('k', x))
        | 'letter gbk' >> beam.GroupByKey()
        | ReverseTestStream(sample_resolution_sec=1, output_tag=None))

    assert_that(
        records,
        equal_to_per_window({
            beam.window.GlobalWindow(): [
                [ProcessingTimeEvent(5), WatermarkEvent(4999998)],
                [
                    ElementEvent(
                        [TimestampedValue(('k', ['a', 'b', 'c']), 4.999999)])
                ],
                [ProcessingTimeEvent(1), WatermarkEvent(5000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(6000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(7000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(8000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(9000000)],
                [
                    ElementEvent(
                        [TimestampedValue(('k', ['1', '2', '3']), 9.999999)])
                ],
                [ProcessingTimeEvent(1), WatermarkEvent(10000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(11000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(12000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(13000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(14000000)],
                [ProcessingTimeEvent(1), WatermarkEvent(15000000)],
            ],
        }))

    p.run()

  def test_basic_execution_in_records_format(self):
    test_stream = (TestStream()
                   .advance_watermark_to(0)
                   .advance_processing_time(5)
                   .add_elements(['a', 'b', 'c'])
                   .advance_watermark_to(2)
                   .advance_processing_time(1)
                   .advance_watermark_to(4)
                   .advance_processing_time(1)
                   .advance_watermark_to(6)
                   .advance_processing_time(1)
                   .advance_watermark_to(8)
                   .advance_processing_time(1)
                   .advance_watermark_to(10)
                   .advance_processing_time(1)
                   .add_elements([TimestampedValue('1', 15),
                                  TimestampedValue('2', 15),
                                  TimestampedValue('3', 15)]))  # yapf: disable

    options = StandardOptions(streaming=True)
    p = TestPipeline(options=options)

    coder = beam.coders.FastPrimitivesCoder()
    records = (
        p
        | test_stream
        | ReverseTestStream(
            sample_resolution_sec=1,
            coder=coder,
            output_format=OutputFormat.TEST_STREAM_FILE_RECORDS,
            output_tag=None)
        | 'stringify' >> beam.Map(str))

    assert_that(
        records,
        equal_to_per_window({
            beam.window.GlobalWindow(): [
                str(TestStreamFileHeader()),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=5000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=0)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            element_event=TestStreamPayload.Event.AddElements(
                                elements=[
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('a'),
                                        timestamp=0),
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('b'),
                                        timestamp=0),
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('c'),
                                        timestamp=0),
                                ])))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=2000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=1000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=4000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=1000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=6000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=1000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=8000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=1000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            watermark_event=TestStreamPayload.Event.
                            AdvanceWatermark(new_watermark=10000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            processing_time_event=TestStreamPayload.Event.
                            AdvanceProcessingTime(advance_duration=1000000)))),
                str(
                    TestStreamFileRecord(
                        recorded_event=TestStreamPayload.Event(
                            element_event=TestStreamPayload.Event.AddElements(
                                elements=[
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('1'),
                                        timestamp=15000000),
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('2'),
                                        timestamp=15000000),
                                    TestStreamPayload.TimestampedElement(
                                        encoded_element=coder.encode('3'),
                                        timestamp=15000000),
                                ])))),
            ],
        }))

    p.run()


if __name__ == '__main__':
  unittest.main()
