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

from __future__ import absolute_import

from abc import ABCMeta
from abc import abstractmethod
from builtins import object
from functools import total_ordering

from future.utils import with_metaclass

from apache_beam import coders
from apache_beam import pvalue
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
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
class Event(with_metaclass(ABCMeta, object)):  # type: ignore[misc]
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

  @abstractmethod
  def to_runner_api(self, element_coder):
    raise NotImplementedError

  @staticmethod
  def from_runner_api(proto, element_coder):
    if proto.HasField('element_event'):
      return ElementEvent([
          TimestampedValue(
              element_coder.decode(tv.encoded_element),
              timestamp.Timestamp(micros=1000 * tv.timestamp))
          for tv in proto.element_event.elements
      ])
    elif proto.HasField('watermark_event'):
      return WatermarkEvent(
          timestamp.Timestamp(
              micros=1000 * proto.watermark_event.new_watermark))
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
    return (
        self.timestamped_values == other.timestamped_values and
        self.tag == other.tag)

  def __hash__(self):
    return hash(self.timestamped_values)

  def __lt__(self, other):
    return self.timestamped_values < other.timestamped_values

  def to_runner_api(self, element_coder):
    return beam_runner_api_pb2.TestStreamPayload.Event(
        element_event=beam_runner_api_pb2.TestStreamPayload.Event.AddElements(
            elements=[
                beam_runner_api_pb2.TestStreamPayload.TimestampedElement(
                    encoded_element=element_coder.encode(tv.value),
                    timestamp=tv.timestamp.micros // 1000)
                for tv in self.timestamped_values
            ]))


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

  def to_runner_api(self, unused_element_coder):
    return beam_runner_api_pb2.TestStreamPayload.Event(
        watermark_event=beam_runner_api_pb2.TestStreamPayload.Event.
        AdvanceWatermark(new_watermark=self.new_watermark.micros // 1000))


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

  def to_runner_api(self, unused_element_coder):
    return beam_runner_api_pb2.TestStreamPayload.Event(
        processing_time_event=beam_runner_api_pb2.TestStreamPayload.Event.
        AdvanceProcessingTime(advance_duration=self.advance_by.micros // 1000))


class TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time. After all of the specified elements are emitted, ceases to produce
  output.
  """
  def __init__(self, coder=coders.FastPrimitivesCoder(), events=None):
    super(TestStream, self).__init__()
    assert coder is not None
    self.coder = coder
    self.watermarks = {None: timestamp.MIN_TIMESTAMP}
    self._events = [] if events is None else list(events)
    self.output_tags = set()

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    self.pipeline = pbegin.pipeline
    return pvalue.PCollection(self.pipeline, is_bounded=False)

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
    return (
        common_urns.primitives.TEST_STREAM.urn,
        beam_runner_api_pb2.TestStreamPayload(
            coder_id=context.coders.get_id(self.coder),
            events=[e.to_runner_api(self.coder) for e in self._events]))

  @staticmethod
  @PTransform.register_urn(
      common_urns.primitives.TEST_STREAM.urn,
      beam_runner_api_pb2.TestStreamPayload)
  def from_runner_api_parameter(payload, context):
    coder = context.coders.get_by_id(payload.coder_id)
    return TestStream(
        coder=coder,
        events=[Event.from_runner_api(e, coder) for e in payload.events])
