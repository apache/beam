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


class TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time. After all of the specified elements are emitted, ceases to produce
  output.
  """
  def __init__(self, coder=coders.FastPrimitivesCoder()):
    assert coder is not None
    self.coder = coder
    self.watermarks = { None: timestamp.MIN_TIMESTAMP }
    self._events = []
    self.output_tags = set()

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pbegin):
    assert(isinstance(pbegin, pvalue.PBegin))
    self.pipeline = pbegin.pipeline

    # This enables multiplexing to multiple output PCollections.
    def mux(event):
      if event.tag:
        yield pvalue.TaggedOutput(event.tag, event)
      else:
        yield event
    mux_output = (pbegin
                  | _TestStream(self.output_tags, events=self._events)
                  | 'TestStream Multiplexer' >> beam.ParDo(mux).with_outputs())

    # Apply a way to control the watermark per output.
    outputs = {}
    for tag in self.output_tags:
      label = '_WatermarkController[{}]'.format(tag)
      outputs[tag] = (mux_output[tag] | label >> _WatermarkController())

    # Downstream consumers expect a PCollection if there is only a single
    # output.
    if len(outputs) == 1:
      return list(outputs.values())[0]
    return pvalue.PTuple(outputs)

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
        if event_timestamp == None:
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


class _WatermarkController(PTransform):
  """A runner-overridable PTransform Primitive to control the watermark.

  Expected implementation behavior:
   - If the instance recieves a WatermarkEvent, it sets its output watermark to
     the specified value then drops the event.
   - If the instance receives an ElementEvent, it emits all specified elements
     to the Global Window with the event time set to the element's timestamp.
  """
  def get_windowing(self, _):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    return pvalue.PCollection.from_(pcoll)


class _TestStream(PTransform):
  """Test stream that generates events on an unbounded PCollection of elements.

  Each event emits elements, advances the watermark or advances the processing
  time.  After all of the specified elements are emitted, ceases to produce
  output.

  Expected implementation behavior:
   - If the instance receives a WatermarkEvent with the WATERMARK_CONTROL_TAG
     then the instance sets its own watermark hold at the specified value and
     drops the event.
   - If the instance receives any other WatermarkEvent or ElementEvent, it
     passes it to the consumer.
  """

  # This tag is used on WatermarkEvents to control the watermark at the root
  # TestStream.
  WATERMARK_CONTROL_TAG = '_TestStream_Watermark'

  def __init__(self, output_tags, coder=coders.FastPrimitivesCoder(),
               events=[]):
    assert coder is not None
    self.coder = coder
    self._events = self._add_watermark_advancements(output_tags, events)

  def _add_watermark_advancements(self, output_tags, events):
    """Adds watermark advancements to the given events.

    The following watermark advancements can be done on the runner side.
    However, it makes the logic on the runner side much more complicated than
    it needs to be.

    In order for watermarks to be properly advanced in a TestStream, a specific
    sequence of watermark holds must be sent:

    1. Hold the root watermark at -inf (this prevents the pipeline from
       immediately returning).
    2. Hold the watermarks at the WatermarkControllerss at -inf (this prevents
       the pipeline from immediately returning).
    3. Advance the root watermark to +inf - 1 (this allows the downstream
       WatermarkControllers to control their watermarks via holds).
    4. Advance watermarks as normal.
    5. Advance WatermarkController watermarks to +inf
    6. Advance root watermark to +inf.
    """
    if not events:
      return []

    watermark_starts = [WatermarkEvent(timestamp.MIN_TIMESTAMP, tag)
                        for tag in output_tags]
    watermark_stops = [WatermarkEvent(timestamp.MAX_TIMESTAMP, tag)
                       for tag in output_tags]

    test_stream_init = [WatermarkEvent(timestamp.MIN_TIMESTAMP,
                                       _TestStream.WATERMARK_CONTROL_TAG)]
    test_stream_start = [WatermarkEvent(
        timestamp.MAX_TIMESTAMP - timestamp.TIME_GRANULARITY,
        _TestStream.WATERMARK_CONTROL_TAG)]
    test_stream_stop = [WatermarkEvent(timestamp.MAX_TIMESTAMP,
                                       _TestStream.WATERMARK_CONTROL_TAG)]

    return (test_stream_init
            + watermark_starts
            + test_stream_start
            + events
            + watermark_stops
            + test_stream_stop)

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline, is_bounded=False)

  def _infer_output_coder(self, input_type=None, input_coder=None):
    return self.coder

  def _events_from_script(self, index):
    yield self._events[index]

  def events(self, index):
    return self._events_from_script(index)

  def begin(self):
    return 0

  def end(self, index):
    return index >= len(self._events)

  def next(self, index):
    return index + 1
