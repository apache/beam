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
from __future__ import absolute_import

from abc import ABCMeta
from abc import abstractmethod
from builtins import object
from collections import namedtuple
from functools import total_ordering

from future.utils import with_metaclass

import apache_beam as beam

from apache_beam import coders
from apache_beam import core
from apache_beam import pvalue
from apache_beam.transforms import PTransform
from apache_beam.transforms import window
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.windowed_value import WindowedValue

__all__ = [
    'Event',
    'ElementEvent',
    'WatermarkEvent',
    'ProcessingTimeEvent',
    'TestStream',
    ]


@total_ordering
class Event(with_metaclass(ABCMeta, object)):
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

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other


class ElementEvent(Event):
  """Element-producing test stream event."""

  def __init__(self, timestamped_values, tag=None):
    self.timestamped_values = timestamped_values
    self.tag = tag

  def __eq__(self, other):
    return (self.timestamped_values == other.timestamped_values and
            self.tag == other.tag)

  def __hash__(self):
    return hash(self.timestamped_values)

  def __lt__(self, other):
    return self.timestamped_values < other.timestamped_values


class WatermarkEvent(Event):
  """Watermark-advancing test stream event."""

  def __init__(self, new_watermark, tag=None):
    self.new_watermark = timestamp.Timestamp.of(new_watermark)
    self.tag = tag

  def __eq__(self, other):
    return self.new_watermark == other.new_watermark and self.tag == other.tag

  def __hash__(self):
    return hash(self.new_watermark)

  def __lt__(self, other):
    return self.new_watermark < other.new_watermark


class ProcessingTimeEvent(Event):
  """Processing time-advancing test stream event."""

  def __init__(self, advance_by):
    self.advance_by = timestamp.Duration.of(advance_by)

  def __eq__(self, other):
    return self.advance_by == other.advance_by

  def __hash__(self):
    return hash(self.advance_by)

  def __lt__(self, other):
    return self.advance_by < other.advance_by


class _WatermarkController(PTransform):
  def get_windowing(self, _):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    return pvalue.PCollection.from_(pcoll)


class _TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time.  After all of the specified elements are emitted, ceases to produce
  output.
  """
  def __init__(self, coder=coders.FastPrimitivesCoder, events=[], endpoint=''):
    assert coder is not None
    self.coder = coder
    self._events = events
    self._endpoint = endpoint
    self._is_done = False

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline, is_bounded=False)

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

  def has_events(self):
    return len(self._events) > 0

  def _events_from_service(self):
    channel = grpc.insecure_channel(self._endpoint)
    stub = beam_interactive_api_pb2_grpc.InteractiveServiceStub(channel)
    request = beam_interactive_api_pb2.EventsRequest()
    for response in stub.Events(request):
      if response.end_of_stream:
        self._is_done = True
      else:
        self._is_done = False
      for event in response.events:
        if event.HasField('watermark_event'):
          yield WatermarkEvent(event.watermark_event.new_watermark)
        elif event.HasField('processing_time_event'):
          yield ProcessingTimeEvent(
              event.processing_time_event.advance_duration)
        elif event.HasField('element_event'):
          for element in event.element_event.elements:
            value = self.coder().decode(element.encoded_element)
            yield ElementEvent([TimestampedValue(value, element.timestamp)])

  def _events_from_script(self, index):
    if len(self._events) == 0:
      return
    yield self._events[index]

  def events(self, index):
    if self._endpoint:
      return self._events_from_service()
    return self._events_from_script(index)

  def begin(self):
    return 0

  def end(self, index):
    if self._endpoint:
      return self._is_done
    return index >= len(self._events)

  def next(self, index):
    if self._endpoint:
      return 0
    return index + 1


class TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time.  After all of the specified elements are emitted, ceases to produce
  output.
  """
  def __init__(self, coder=coders.FastPrimitivesCoder, endpoint=''):
    assert coder is not None
    self.coder = coder
    self.watermarks = { None: timestamp.MIN_TIMESTAMP }
    self._events = []
    self._endpoint = endpoint
    self.output_tags = set()

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def with_outputs(self, *tags, **main_kw):
    main_tag = main_kw.pop('main', None)
    if main_kw:
      raise ValueError('Unexpected keyword arguments: %s' %
                       list(main_kw))
    for tag in tags:
      self.output_tags.add(tag)
    return namedtuple('Output', list(self.output_tags))

  def expand(self, pbegin):
    assert(isinstance(pbegin, pvalue.PBegin))
    self.pipeline = pbegin.pipeline
    self.outputs = {}

    watermark_starts = []
    watermark_stops = []
    for tag in self.output_tags:
      event = WatermarkEvent(
          timestamp.MIN_TIMESTAMP + timestamp.TIME_GRANULARITY, tag)
      watermark_starts.append(event)

      event = WatermarkEvent(timestamp.MAX_TIMESTAMP, tag)
      watermark_stops.append(event)
    self._events = watermark_starts + self._events + watermark_stops

    def mux(x):
      assert(isinstance(x, (ElementEvent, WatermarkEvent)),
             'Received unknown TestStream event')
      yield pvalue.TaggedOutput(x.tag, x)

    mux_output = (pbegin
                  | _TestStream(events=self._events)
                  | 'TestStream Multiplexer' >> beam.ParDo(mux).with_outputs())

    for tag in self.output_tags:
      label = '_WatermarkController[{}]'.format(tag)
      self.outputs[tag] = (mux_output[tag] | label >> _WatermarkController())
    tuple_names = list(self.output_tags)
    return namedtuple('TestStreamOutput', tuple_names)(*self.outputs.values())

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

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
    for element in elements:
      if isinstance(element, TimestampedValue):
        timestamped_values.append(element)
      elif isinstance(element, WindowedValue):
        # Drop windows for elements in test stream.
        timestamped_values.append(
            TimestampedValue(element.value, element.timestamp))
      else:
        # Add elements with timestamp equal to current watermark.
        timestamped_values.append(
            TimestampedValue(element, self.current_watermark))
    self._add(ElementEvent(timestamped_values))
    return self

  def advance_watermark_to(self, new_watermark):
    """Advance the watermark to a given Unix timestamp.

    The Unix timestamp value used must be later than the previous watermark
    value and should be given as an int, float or utils.timestamp.Timestamp
    object.
    """
    self.output_tags.add(tag)
    self._add(WatermarkEvent(new_watermark, tag))
    return self

  def advance_watermark_to_infinity(self):
    """Advance the watermark to the end of time, completing this TestStream."""
    self.advance_watermark_to(timestamp.MAX_TIMESTAMP)
    return self

  def advance_processing_time(self, advance_by):
    """Advance the current processing time by a given duration in seconds.

    The duration must be a positive second duration and should be given as an
    int, float or utils.timestamp.Duration object.
    """
    self._add(ProcessingTimeEvent(advance_by))
    return self
