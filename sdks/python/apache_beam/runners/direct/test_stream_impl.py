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
from __future__ import print_function

import itertools
import logging

from apache_beam import ParDo
from apache_beam import coders
from apache_beam import pvalue
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.testing.test_stream import ElementEvent
from apache_beam.testing.test_stream import ProcessingTimeEvent
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

try:
  import grpc
  from apache_beam.portability.api import beam_runner_api_pb2_grpc  # pylint: disable=ungrouped-imports
except ImportError:
  grpc = None
  beam_runner_api_pb2_grpc = None
  # A workaround for directrunner users who would cannot depend
  # on grpc which are missing grpc dependencyy.
  print(
      'Exception: grpc was not able to be imported. '
      'Skip importing all grpc related moduels.')

_LOGGER = logging.getLogger(__name__)


class _WatermarkController(PTransform):
  """A runner-overridable PTransform Primitive to control the watermark.

  Expected implementation behavior:
   - If the instance recieves a WatermarkEvent, it sets its output watermark to
     the specified value then drops the event.
   - If the instance receives an ElementEvent, it emits all specified elements
     to the Global Window with the event time set to the element's timestamp.
  """
  def __init__(self, output_tag):
    self.output_tag = output_tag

  def get_windowing(self, _):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    ret = pvalue.PCollection.from_(pcoll)
    ret.tag = self.output_tag
    return ret


class _ExpandableTestStream(PTransform):
  def __init__(self, test_stream):
    self.test_stream = test_stream

  def expand(self, pbegin):
    """Expands the TestStream into the DirectRunner implementation.

    Takes the TestStream transform and creates a _TestStream -> multiplexer ->
    _WatermarkController.
    """

    assert isinstance(pbegin, pvalue.PBegin)

    # If there is only one tag there is no need to add the multiplexer.
    if len(self.test_stream.output_tags) == 1:
      return (
          pbegin
          | _TestStream(
              self.test_stream.output_tags,
              events=self.test_stream._events,
              coder=self.test_stream.coder,
              endpoint=self.test_stream._endpoint)
          | _WatermarkController(list(self.test_stream.output_tags)[0]))

    # Multiplex to the correct PCollection based upon the event tag.
    def mux(event):
      if event.tag:
        yield pvalue.TaggedOutput(event.tag, event)
      else:
        yield event

    mux_output = (
        pbegin
        | _TestStream(
            self.test_stream.output_tags,
            events=self.test_stream._events,
            coder=self.test_stream.coder,
            endpoint=self.test_stream._endpoint)
        | 'TestStream Multiplexer' >> ParDo(mux).with_outputs())

    # Apply a way to control the watermark per output. It is necessary to
    # have an individual _WatermarkController per PCollection because the
    # calculation of the input watermark of a transform is based on the event
    # timestamp of the elements flowing through it. Meaning, it is impossible
    # to control the output watermarks of the individual PCollections solely
    # on the event timestamps.
    outputs = {}
    for tag in self.test_stream.output_tags:
      label = '_WatermarkController[{}]'.format(tag)
      outputs[tag] = (mux_output[tag] | label >> _WatermarkController(tag))

    return outputs


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

  def __init__(
      self,
      output_tags,
      coder=coders.FastPrimitivesCoder(),
      events=None,
      endpoint=None):
    assert coder is not None
    self.coder = coder
    self._raw_events = events
    self._events = self._add_watermark_advancements(output_tags, events)
    self.output_tags = output_tags
    self.endpoint = endpoint

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
    return [
        WatermarkEvent(
            timestamp.MAX_TIMESTAMP - timestamp.TIME_GRANULARITY,
            _TestStream.WATERMARK_CONTROL_TAG)
    ]

  def _test_stream_stop(self):
    """Sentinel value to close the watermark of the TestStream."""
    return [
        WatermarkEvent(
            timestamp.MAX_TIMESTAMP, _TestStream.WATERMARK_CONTROL_TAG)
    ]

  def _test_stream_init(self):
    """Sentinel value to hold the watermark of the TestStream to -inf.

    This sets a hold to ensure that the output watermarks of the output
    PCollections do not advance to +inf before their watermark holds are set.
    """
    return [
        WatermarkEvent(
            timestamp.MIN_TIMESTAMP, _TestStream.WATERMARK_CONTROL_TAG)
    ]

  def _set_up(self, output_tags):
    return (
        self._test_stream_init() + self._watermark_starts(output_tags) +
        self._test_stream_start())

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

  @staticmethod
  def events_from_script(events):
    """Yields the in-memory events.
    """
    return itertools.chain(events)

  @staticmethod
  def events_from_rpc(endpoint, output_tags, coder):
    """Yields the events received from the given endpoint.
    """
    stub_channel = grpc.insecure_channel(endpoint)
    stub = beam_runner_api_pb2_grpc.TestStreamServiceStub(stub_channel)

    # Request the PCollections that we are looking for from the service.
    event_request = beam_runner_api_pb2.EventsRequest(
        output_ids=[str(tag) for tag in output_tags])

    event_stream = stub.Events(event_request, timeout=30)
    try:
      while True:
        yield _TestStream.test_stream_payload_to_events(
            next(event_stream), coder)
    except StopIteration:
      return
    except grpc.RpcError as e:
      if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        _LOGGER.warning(
            'TestStream timed out waiting for new events from service.'
            ' Stopping pipeline.')
        return
      raise e

  @staticmethod
  def test_stream_payload_to_events(payload, coder):
    """Returns a TestStream Python event object from a TestStream event Proto.
    """
    if payload.HasField('element_event'):
      element_event = payload.element_event
      elements = [
          TimestampedValue(
              coder.decode(e.encoded_element), Timestamp(micros=e.timestamp))
          for e in element_event.elements
      ]
      return ElementEvent(timestamped_values=elements, tag=element_event.tag)

    if payload.HasField('watermark_event'):
      watermark_event = payload.watermark_event
      return WatermarkEvent(
          Timestamp(micros=watermark_event.new_watermark),
          tag=watermark_event.tag)

    if payload.HasField('processing_time_event'):
      processing_time_event = payload.processing_time_event
      return ProcessingTimeEvent(
          Duration(micros=processing_time_event.advance_duration))

    raise RuntimeError(
        'Received a proto without the specified fields: {}'.format(payload))
