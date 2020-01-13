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

"""The TestStream implementation for the DirectRunner

The DirectRunner implements TestStream as the _TestStream class which is used
to store the events in memory, the _WatermarkController which is used to set the
watermark and emit events, and the multiplexer which sends events to the correct
tagged PCollection.
"""

# pytype: skip-file

from __future__ import absolute_import

from apache_beam import coders
from apache_beam import pvalue
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.utils import timestamp


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
               events=None):
    assert coder is not None
    self.coder = coder
    self._raw_events = events
    self._events = self._add_watermark_advancements(output_tags, events)

  def _watermark_starts(self, output_tags):
    """Sentinel values to hold the watermark of outputs to -inf.

    The output watermarks of the output PCollections (fake unbounded sources) in
    a TestStream are controlled by watermark holds. This sets the hold of each
    output PCollection so that the individual holds can be controlled by the
    given events.
    """
    return [WatermarkEvent(timestamp.MIN_TIMESTAMP, tag) for tag in output_tags]

  def _watermark_stops(self, output_tags):
    """Sentinel values to close the watermark of outputs."""
    return [WatermarkEvent(timestamp.MAX_TIMESTAMP, tag) for tag in output_tags]

  def _test_stream_start(self):
    """Sentinel value to move the watermark hold of the TestStream to +inf.

    This sets a hold to +inf such that the individual holds of the output
    PCollections are allowed to modify their individial output watermarks with
    their holds. This is because the calculation of the output watermark is a
    min over all input watermarks.
    """
    return [WatermarkEvent(timestamp.MAX_TIMESTAMP - timestamp.TIME_GRANULARITY,
                           _TestStream.WATERMARK_CONTROL_TAG)]

  def _test_stream_stop(self):
    """Sentinel value to close the watermark of the TestStream."""
    return [WatermarkEvent(timestamp.MAX_TIMESTAMP,
                           _TestStream.WATERMARK_CONTROL_TAG)]

  def _test_stream_init(self):
    """Sentinel value to hold the watermark of the TestStream to -inf.

    This sets a hold to ensure that the output watermarks of the output
    PCollections do not advance to +inf before their watermark holds are set.
    """
    return [WatermarkEvent(timestamp.MIN_TIMESTAMP,
                           _TestStream.WATERMARK_CONTROL_TAG)]

  def _set_up(self, output_tags):
    return (self._test_stream_init()
            + self._watermark_starts(output_tags)
            + self._test_stream_start())

  def _tear_down(self, output_tags):
    return self._watermark_stops(output_tags) + self._test_stream_stop()

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

    return self._set_up(output_tags) + events + self._tear_down(output_tags)

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
