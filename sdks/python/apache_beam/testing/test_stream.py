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

"""Provides TestStream for verifying streaming runner semantics.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from abc import ABCMeta
from abc import abstractmethod
from enum import Enum
from functools import total_ordering

import apache_beam as beam
from apache_beam import coders
from apache_beam import pvalue
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileHeader
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.portability.api.endpoints_pb2 import ApiServiceDescriptor
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue

__all__ = [
    'Event',
    'ElementEvent',
    'WatermarkEvent',
    'ProcessingTimeEvent',
    'TestStream',
]


@total_ordering
class Event(metaclass=ABCMeta):  # type: ignore[misc]
  """Test stream event to be emitted during execution of a TestStream."""
  @abstractmethod
  def __eq__(self, other):
    raise NotImplementedError

  @abstractmethod
  def __hash__(self):
    raise NotImplementedError

  @abstractmethod
  def __lt__(self, other):
    raise NotImplementedError

  @abstractmethod
  def to_runner_api(self, element_coder):
    raise NotImplementedError

  @staticmethod
  def from_runner_api(proto, element_coder):
    if proto.HasField('element_event'):
      event = proto.element_event
      tag = None if event.tag == 'None' else event.tag
      return ElementEvent([
          TimestampedValue(
              element_coder.decode(tv.encoded_element),
              Timestamp(micros=1000 * tv.timestamp))
          for tv in proto.element_event.elements
      ], tag=tag) # yapf: disable
    elif proto.HasField('watermark_event'):
      event = proto.watermark_event
      tag = None if event.tag == 'None' else event.tag
      return WatermarkEvent(
          Timestamp(micros=1000 * proto.watermark_event.new_watermark), tag=tag)
    elif proto.HasField('processing_time_event'):
      return ProcessingTimeEvent(
          timestamp.Duration(
              micros=1000 * proto.processing_time_event.advance_duration))
    else:
      raise ValueError(
          'Unknown TestStream Event type: %s' % proto.WhichOneof('event'))


class ElementEvent(Event):
  """Element-producing test stream event."""
  def __init__(self, timestamped_values, tag=None):
    self.timestamped_values = timestamped_values
    self.tag = tag

  def __eq__(self, other):
    if not isinstance(other, ElementEvent):
      return False

    return (
        self.timestamped_values == other.timestamped_values and
        self.tag == other.tag)

  def __hash__(self):
    return hash(self.timestamped_values)

  def __lt__(self, other):
    if not isinstance(other, ElementEvent):
      raise TypeError

    return self.timestamped_values < other.timestamped_values

  def to_runner_api(self, element_coder):
    tag = 'None' if self.tag is None else self.tag
    return beam_runner_api_pb2.TestStreamPayload.Event(
        element_event=beam_runner_api_pb2.TestStreamPayload.Event.AddElements(
            elements=[
                beam_runner_api_pb2.TestStreamPayload.TimestampedElement(
                    encoded_element=element_coder.encode(tv.value),
                    timestamp=tv.timestamp.micros // 1000)
                for tv in self.timestamped_values
            ],
            tag=tag))

  def __repr__(self):
    return 'ElementEvent: <{}, {}>'.format([(e.value, e.timestamp)
                                            for e in self.timestamped_values],
                                           self.tag)


class WatermarkEvent(Event):
  """Watermark-advancing test stream event."""
  def __init__(self, new_watermark, tag=None):
    self.new_watermark = Timestamp.of(new_watermark)
    self.tag = tag

  def __eq__(self, other):
    if not isinstance(other, WatermarkEvent):
      return False

    return self.new_watermark == other.new_watermark and self.tag == other.tag

  def __hash__(self):
    return hash(str(self.new_watermark) + str(self.tag))

  def __lt__(self, other):
    if not isinstance(other, WatermarkEvent):
      raise TypeError

    return self.new_watermark < other.new_watermark

  def to_runner_api(self, unused_element_coder):
    tag = 'None' if self.tag is None else self.tag

    # Assert that no precision is lost.
    assert self.new_watermark.micros % 1000 == 0
    return beam_runner_api_pb2.TestStreamPayload.Event(
        watermark_event=beam_runner_api_pb2.TestStreamPayload.Event.
        AdvanceWatermark(
            new_watermark=self.new_watermark.micros // 1000, tag=tag))

  def __repr__(self):
    return 'WatermarkEvent: <{}, {}>'.format(self.new_watermark, self.tag)


class ProcessingTimeEvent(Event):
  """Processing time-advancing test stream event."""
  def __init__(self, advance_by):
    self.advance_by = Duration.of(advance_by)

  def __eq__(self, other):
    if not isinstance(other, ProcessingTimeEvent):
      return False

    return self.advance_by == other.advance_by

  def __hash__(self):
    return hash(self.advance_by)

  def __lt__(self, other):
    if not isinstance(other, ProcessingTimeEvent):
      raise TypeError

    return self.advance_by < other.advance_by

  def to_runner_api(self, unused_element_coder):
    assert self.advance_by.micros % 1000 == 0
    return beam_runner_api_pb2.TestStreamPayload.Event(
        processing_time_event=beam_runner_api_pb2.TestStreamPayload.Event.
        AdvanceProcessingTime(advance_duration=self.advance_by.micros // 1000))

  def __repr__(self):
    return 'ProcessingTimeEvent: <{}>'.format(self.advance_by)


class WindowedValueHolderMeta(type):
  """A metaclass that overrides the isinstance check for WindowedValueHolder.

  Python does a quick test for exact match. If an instance is exactly of
  type WindowedValueHolder, the overridden isinstance check is omitted.
  The override is needed because WindowedValueHolder elements encoded then
  decoded become Row elements.
  """
  def __instancecheck__(cls, other):
    """Checks if a beam.Row typed instance is a WindowedValueHolder.
    """
    return (
        isinstance(other, beam.Row) and hasattr(other, 'windowed_value') and
        hasattr(other, 'urn') and
        isinstance(other.windowed_value, WindowedValue) and
        other.urn == common_urns.coders.ROW.urn)


class WindowedValueHolder(beam.Row, metaclass=WindowedValueHolderMeta):
  """A class that holds a WindowedValue.

  This is a special class that can be used by the runner that implements the
  TestStream as a signal that the underlying value should be unreified to the
  specified window.
  """
  # Register WindowedValueHolder to always use RowCoder.
  coders.registry.register_coder(WindowedValueHolderMeta, coders.RowCoder)

  def __init__(self, windowed_value):
    assert isinstance(windowed_value, WindowedValue), (
        'WindowedValueHolder can only hold %s type. Instead, %s is given.') % (
            WindowedValue, windowed_value)
    super().__init__(
        **{
            'windowed_value': windowed_value, 'urn': common_urns.coders.ROW.urn
        })

  @classmethod
  def from_row(cls, row):
    """Converts a beam.Row typed instance to WindowedValueHolder.
    """
    if isinstance(row, WindowedValueHolder):
      return WindowedValueHolder(row.windowed_value)
    assert isinstance(row, beam.Row), 'The given row %s must be a %s type' % (
        row, beam.Row)
    assert hasattr(row, 'windowed_value'), (
        'The given %s must have a windowed_value attribute.') % row
    assert isinstance(row.windowed_value, WindowedValue), (
        'The windowed_value attribute of %s must be a %s type') % (
            row, WindowedValue)


class TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time. After all of the specified elements are emitted, ceases to produce
  output.

  Applying the PTransform will return a single PCollection if only the default
  output or only one output tag has been used. Otherwise a dictionary of output
  names to PCollections will be returned.
  """
  def __init__(
      self,
      coder=coders.FastPrimitivesCoder(),
      events=None,
      output_tags=None,
      endpoint=None):
    """
    Args:
      coder: (apache_beam.Coder) the coder to encode/decode elements.
      events: (List[Event]) a list of instructions for the TestStream to
        execute. If specified, the events tags must exist in the output_tags.
      output_tags: (List[str]) Initial set of outputs. If no event references an
        output tag, no output will be produced for that tag.
      endpoint: (str) a URL locating a TestStreamService.
    """

    super().__init__()
    assert coder is not None

    self.coder = coder
    self.watermarks = {None: timestamp.MIN_TIMESTAMP}
    self.output_tags = set(output_tags) if output_tags else set()
    self._events = [] if events is None else list(events)
    self._endpoint = endpoint

    event_tags = set(
        e.tag for e in self._events
        if isinstance(e, (WatermarkEvent, ElementEvent)))
    assert event_tags.issubset(self.output_tags), \
        '{} is not a subset of {}'.format(event_tags, output_tags)
    assert not (self._events and self._endpoint), \
        'Only either events or an endpoint can be given at once.'

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    self.pipeline = pbegin.pipeline
    if not self.output_tags:
      self.output_tags = {None}

    # For backwards compatibility return a single PCollection.
    if self.output_tags == {None}:
      return pvalue.PCollection(
          self.pipeline, is_bounded=False, tag=list(self.output_tags)[0])
    return {
        tag: pvalue.PCollection(self.pipeline, is_bounded=False, tag=tag)
        for tag in self.output_tags
    }

  def _add(self, event):
    if isinstance(event, ElementEvent):
      for tv in event.timestamped_values:
        assert tv.timestamp < timestamp.MAX_TIMESTAMP, (
            'Element timestamp must be before timestamp.MAX_TIMESTAMP.')
    elif isinstance(event, WatermarkEvent):
      if event.tag not in self.watermarks:
        self.watermarks[event.tag] = timestamp.MIN_TIMESTAMP
      assert event.new_watermark > self.watermarks[event.tag], (
          'Watermark must strictly-monotonically advance.')
      self.watermarks[event.tag] = event.new_watermark
    elif isinstance(event, ProcessingTimeEvent):
      assert event.advance_by > 0, (
          'Must advance processing time by positive amount.')
    else:
      raise ValueError('Unknown event: %s' % event)
    self._events.append(event)

  def add_elements(self, elements, tag=None, event_timestamp=None):
    """Add elements to the TestStream.

    Elements added to the TestStream will be produced during pipeline execution.
    These elements can be TimestampedValue, WindowedValue or raw unwrapped
    elements that are serializable using the TestStream's specified Coder.  When
    a TimestampedValue or a WindowedValue element is used, the timestamp of the
    TimestampedValue or WindowedValue will be the timestamp of the produced
    element; otherwise, the current watermark timestamp will be used for that
    element.  The windows of a given WindowedValue are ignored by the
    TestStream.
    """
    self.output_tags.add(tag)
    timestamped_values = []
    if tag not in self.watermarks:
      self.watermarks[tag] = timestamp.MIN_TIMESTAMP

    for element in elements:
      if isinstance(element, TimestampedValue):
        timestamped_values.append(element)
      elif isinstance(element, WindowedValue):
        # Drop windows for elements in test stream.
        timestamped_values.append(
            TimestampedValue(element.value, element.timestamp))
      else:
        # Add elements with timestamp equal to current watermark.
        if event_timestamp is None:
          event_timestamp = self.watermarks[tag]
        timestamped_values.append(TimestampedValue(element, event_timestamp))
    self._add(ElementEvent(timestamped_values, tag))
    return self

  def advance_watermark_to(self, new_watermark, tag=None):
    """Advance the watermark to a given Unix timestamp.

    The Unix timestamp value used must be later than the previous watermark
    value and should be given as an int, float or utils.timestamp.Timestamp
    object.
    """
    self.output_tags.add(tag)
    self._add(WatermarkEvent(new_watermark, tag))
    return self

  def advance_watermark_to_infinity(self, tag=None):
    """Advance the watermark to the end of time, completing this TestStream."""
    self.advance_watermark_to(timestamp.MAX_TIMESTAMP, tag)
    return self

  def advance_processing_time(self, advance_by):
    """Advance the current processing time by a given duration in seconds.

    The duration must be a positive second duration and should be given as an
    int, float or utils.timestamp.Duration object.
    """
    self._add(ProcessingTimeEvent(advance_by))
    return self

  def to_runner_api_parameter(self, context):
    # Sort the output tags so that the order is deterministic and we are able
    # to test equality on a roundtrip through the to/from proto apis.
    return (
        common_urns.primitives.TEST_STREAM.urn,
        beam_runner_api_pb2.TestStreamPayload(
            coder_id=context.coders.get_id(self.coder),
            events=[e.to_runner_api(self.coder) for e in self._events],
            endpoint=ApiServiceDescriptor(url=self._endpoint)))

  @staticmethod
  @PTransform.register_urn(
      common_urns.primitives.TEST_STREAM.urn,
      beam_runner_api_pb2.TestStreamPayload)
  def from_runner_api_parameter(ptransform, payload, context):
    coder = context.coders.get_by_id(payload.coder_id)
    output_tags = set(
        None if k == 'None' else k for k in ptransform.outputs.keys())
    return TestStream(
        coder=coder,
        events=[Event.from_runner_api(e, coder) for e in payload.events],
        output_tags=output_tags,
        endpoint=payload.endpoint.url)


class TimingInfo(object):
  def __init__(self, processing_time, watermark):
    self._processing_time = Timestamp.of(processing_time)
    self._watermark = Timestamp.of(watermark)

  @property
  def processing_time(self):
    return self._processing_time

  @property
  def watermark(self):
    return self._watermark

  def __repr__(self):
    return '({}, {})'.format(self.processing_time, self.watermark)


class PairWithTiming(PTransform):
  """Pairs the input element with timing information.

  Input: element; output: KV(element, timing information)
  Where timing information := (processing time, watermark)

  This is used in the ReverseTestStream implementation to replay watermark
  advancements.
  """

  URN = "beam:transform:pair_with_timing:v1"

  def expand(self, pcoll):
    return pvalue.PCollection.from_(pcoll)


class OutputFormat(Enum):
  TEST_STREAM_EVENTS = 1
  TEST_STREAM_FILE_RECORDS = 2
  SERIALIZED_TEST_STREAM_FILE_RECORDS = 3


class ReverseTestStream(PTransform):
  """A Transform that can create TestStream events from a stream of elements.

  This currently assumes that this the pipeline being run on a single machine
  and elements come in order and are outputted in the same order that they came
  in.
  """
  def __init__(
      self, sample_resolution_sec, output_tag, coder=None, output_format=None):
    self._sample_resolution_sec = sample_resolution_sec
    self._output_tag = output_tag
    self._output_format = output_format if output_format \
                          else OutputFormat.TEST_STREAM_EVENTS
    self._coder = coder if coder else beam.coders.FastPrimitivesCoder()

  def expand(self, pcoll):
    ret = (
        pcoll
        | beam.WindowInto(beam.window.GlobalWindows())

        # First get the initial timing information. This will be used to start
        # the periodic timers which will generate processing time and watermark
        # advancements every `sample_resolution_sec`.
        | 'initial timing' >> PairWithTiming()

        # Next, map every element to the same key so that only a single timer is
        # started for this given ReverseTestStream.
        | 'first key' >> beam.Map(lambda x: (0, x))

        # Next, pass-through each element which will be paired with its timing
        # info in the next step. Also, start the periodic timers. We use timers
        # in this situation to capture watermark advancements that occur when
        # there are no elements being produced upstream.
        | beam.ParDo(
            _TimingEventGenerator(
                output_tag=self._output_tag,
                sample_resolution_sec=self._sample_resolution_sec))

        # Next, retrieve the timing information for watermark events that were
        # generated in the previous step. This is because elements generated
        # through the timers don't have their timing information yet.
        | 'timing info for watermarks' >> PairWithTiming()

        # Re-key to the same key to keep global state.
        | 'second key' >> beam.Map(lambda x: (0, x))

        # Format the events properly.
        | beam.ParDo(_TestStreamFormatter(self._coder, self._output_format)))

    if self._output_format == OutputFormat.SERIALIZED_TEST_STREAM_FILE_RECORDS:

      def serializer(e):
        return e.SerializeToString()

      ret = ret | 'serializer' >> beam.Map(serializer)

    return ret


class _TimingEventGenerator(beam.DoFn):
  """Generates ProcessingTimeEvents and WatermarkEvents at a regular cadence.

  The runner keeps the state of the clock (which may be faked) and the
  watermarks, which are inaccessible to SDKs. This DoFn generates
  ProcessingTimeEvents and WatermarkEvents at a specified sampling rate to
  capture any clock or watermark advancements between elements.
  """

  # Used to return the initial timing information.
  EXECUTE_ONCE_STATE = beam.transforms.userstate.BagStateSpec(
      name='execute_once_state', coder=beam.coders.FastPrimitivesCoder())

  # A processing time timer in an infinite loop that generates the events that
  # will be paired with the TimingInfo from the runner.
  TIMING_SAMPLER = TimerSpec('timing_sampler', TimeDomain.REAL_TIME)

  def __init__(self, output_tag, sample_resolution_sec=0.1):
    self._output_tag = output_tag
    self._sample_resolution_sec = sample_resolution_sec

  @on_timer(TIMING_SAMPLER)
  def on_timing_sampler(
      self,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam,
      timing_sampler=beam.DoFn.TimerParam(TIMING_SAMPLER)):
    """Yields an unbounded stream of ProcessingTimeEvents and WatermarkEvents.

    The returned events will be paired with the TimingInfo. This loop's only
    purpose is to generate these events even when there are no elements.
    """
    next_sample_time = (timestamp.micros * 1e-6) + self._sample_resolution_sec
    timing_sampler.set(next_sample_time)

    # Generate two events, the delta since the last sample and a place-holder
    # WatermarkEvent. This is a placeholder because we can't otherwise add the
    # watermark from the runner to the event.
    yield ProcessingTimeEvent(self._sample_resolution_sec)
    yield WatermarkEvent(MIN_TIMESTAMP)

  def process(
      self,
      e,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam,
      timing_sampler=beam.DoFn.TimerParam(TIMING_SAMPLER),
      execute_once_state=beam.DoFn.StateParam(EXECUTE_ONCE_STATE)):

    _, (element, timing_info) = e

    # Only set the timers once and only send the header once.
    first_time = next(execute_once_state.read(), True)
    if first_time:
      # Generate the initial timing events.
      execute_once_state.add(False)
      now_sec = timing_info.processing_time.micros * 1e-6
      timing_sampler.set(now_sec + self._sample_resolution_sec)

      # Here we capture the initial time offset and initial watermark. This is
      # where we emit the TestStreamFileHeader.
      yield TestStreamFileHeader(tag=self._output_tag)
      yield ProcessingTimeEvent(
          Duration(micros=timing_info.processing_time.micros))
      yield WatermarkEvent(MIN_TIMESTAMP)
    yield element


class _TestStreamFormatter(beam.DoFn):
  """Formats the events to the specified output format.
  """

  # In order to generate the processing time deltas, we need to keep track of
  # the previous clock time we got from the runner.
  PREV_SAMPLE_TIME_STATE = beam.transforms.userstate.BagStateSpec(
      name='prev_sample_time_state', coder=beam.coders.FastPrimitivesCoder())

  def __init__(self, coder, output_format):
    self._coder = coder
    self._output_format = output_format

  def start_bundle(self):
    self.elements = []
    self.timing_events = []
    self.header = None

  def finish_bundle(self):
    """Outputs all the buffered elements.
    """
    if self._output_format == OutputFormat.TEST_STREAM_EVENTS:
      return self._output_as_events()
    return self._output_as_records()

  def process(
      self,
      e,
      timestamp=beam.DoFn.TimestampParam,
      prev_sample_time_state=beam.DoFn.StateParam(PREV_SAMPLE_TIME_STATE)):
    """Buffers elements until the end of the bundle.

    This buffers elements instead of emitting them immediately to keep elements
    that come in the same bundle to be outputted in the same bundle.
    """
    _, (element, timing_info) = e

    if isinstance(element, TestStreamFileHeader):
      self.header = element
    elif isinstance(element, WatermarkEvent):
      # WatermarkEvents come in with a watermark of MIN_TIMESTAMP. Fill in the
      # correct watermark from the runner here.
      element.new_watermark = timing_info.watermark.micros
      if element not in self.timing_events:
        self.timing_events.append(element)

    elif isinstance(element, ProcessingTimeEvent):
      # Because the runner holds the clock, calculate the processing time delta
      # here. The TestStream may have faked out the clock, and thus the
      # delta calculated in the SDK with time.time() will be wrong.
      prev_sample = next(prev_sample_time_state.read(), Timestamp())
      prev_sample_time_state.clear()
      prev_sample_time_state.add(timing_info.processing_time)

      advance_by = timing_info.processing_time - prev_sample

      element.advance_by = advance_by
      self.timing_events.append(element)
    else:
      self.elements.append(TimestampedValue(element, timestamp))

  def _output_as_events(self):
    """Outputs buffered elements as TestStream events.
    """
    if self.timing_events:
      yield WindowedValue(
          self.timing_events, timestamp=0, windows=[beam.window.GlobalWindow()])

    if self.elements:
      yield WindowedValue([ElementEvent(self.elements)],
                          timestamp=0,
                          windows=[beam.window.GlobalWindow()])

  def _output_as_records(self):
    """Outputs buffered elements as TestStreamFileRecords.
    """
    if self.header:
      yield WindowedValue(
          self.header, timestamp=0, windows=[beam.window.GlobalWindow()])

    if self.timing_events:
      timing_events = self._timing_events_to_records(self.timing_events)
      for r in timing_events:
        yield WindowedValue(
            r, timestamp=0, windows=[beam.window.GlobalWindow()])

    if self.elements:
      elements = self._elements_to_record(self.elements)
      yield WindowedValue(
          elements, timestamp=0, windows=[beam.window.GlobalWindow()])

  def _timing_events_to_records(self, timing_events):
    """Returns given timing_events as TestStreamFileRecords.
    """
    records = []
    for e in self.timing_events:
      if isinstance(e, ProcessingTimeEvent):
        processing_time_event = TestStreamPayload.Event.AdvanceProcessingTime(
            advance_duration=e.advance_by.micros)
        records.append(
            TestStreamFileRecord(
                recorded_event=TestStreamPayload.Event(
                    processing_time_event=processing_time_event)))

      elif isinstance(e, WatermarkEvent):
        watermark_event = TestStreamPayload.Event.AdvanceWatermark(
            new_watermark=int(e.new_watermark))
        records.append(
            TestStreamFileRecord(
                recorded_event=TestStreamPayload.Event(
                    watermark_event=watermark_event)))

    return records

  def _elements_to_record(self, elements):
    """Returns elements as TestStreamFileRecords.
    """
    elements = []
    for tv in self.elements:
      element_timestamp = tv.timestamp.micros
      element = beam_runner_api_pb2.TestStreamPayload.TimestampedElement(
          encoded_element=self._coder.encode(tv.value),
          timestamp=element_timestamp)
      elements.append(element)

    element_event = TestStreamPayload.Event.AddElements(elements=elements)
    return TestStreamFileRecord(
        recorded_event=TestStreamPayload.Event(element_event=element_event))
