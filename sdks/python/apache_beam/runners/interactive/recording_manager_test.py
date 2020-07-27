#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import sys
import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.interactive.recording_manager import ElementStream
from apache_beam.runners.interactive.recording_manager import Recording
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.runners.interactive.testing.test_cache_manager import FileRecordsBuilder
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.windowed_value import WindowedValue

PipelineState = beam.runners.runner.PipelineState


class MockPipelineResult(beam.runners.runner.PipelineResult):
  """Mock class for controlling a PipelineResult."""
  def __init__(self):
    self._state = PipelineState.RUNNING

  def wait_until_finish(self):
    pass

  def set_state(self, state):
    self._state = state

  @property
  def state(self):
    return self._state

  def cancel(self):
    self._state = PipelineState.CANCELLED


class ElementStreamTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

    self.cache = InMemoryCache()
    self.p = beam.Pipeline()
    self.pcoll = self.p | beam.Create([])
    self.cache_key = str(pi.CacheKey('pcoll', '', '', ''))

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    self.mock_result = MockPipelineResult()
    ie.current_env().track_user_pipelines()
    ie.current_env().set_pipeline_result(self.p, self.mock_result)
    ie.current_env().set_cache_manager(self.cache, self.p)

  def test_read(self):
    """Test reading and if a stream is done no more elements are returned."""

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(['expected'], 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=1, max_duration_secs=1)

    self.assertFalse(stream.is_done())
    self.assertEqual(list(stream.read())[0], 'expected')
    self.assertTrue(stream.is_done())

  def test_done_if_terminated(self):
    """Test that terminating the job sets the stream as done."""

    self.cache.write(['expected'], 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=10)

    self.assertFalse(stream.is_done())
    self.assertEqual(list(stream.read(tail=False))[0], 'expected')

    # The limiters were not reached, so the stream is not done yet.
    self.assertFalse(stream.is_done())

    self.mock_result.set_state(PipelineState.DONE)
    self.assertEqual(list(stream.read(tail=False))[0], 'expected')

    # The underlying pipeline is terminated, so the stream won't yield new
    # elements.
    self.assertTrue(stream.is_done())

  def test_read_n(self):
    """Test that the stream only reads 'n' elements."""

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(list(range(5)), 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=1, max_duration_secs=1)
    self.assertEqual(list(stream.read()), [0])
    self.assertTrue(stream.is_done())

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=2, max_duration_secs=1)
    self.assertEqual(list(stream.read()), [0, 1])
    self.assertTrue(stream.is_done())

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=5, max_duration_secs=1)
    self.assertEqual(list(stream.read()), list(range(5)))
    self.assertTrue(stream.is_done())

    # Test that if the user asks for more than in the cache it still returns.
    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=10, max_duration_secs=1)
    self.assertEqual(list(stream.read()), list(range(5)))
    self.assertTrue(stream.is_done())

  def test_read_duration(self):
    """Test that the stream only reads a 'duration' of elements."""

    values = (FileRecordsBuilder(tag=self.cache_key)
              .advance_processing_time(1)
              .add_element(element=0, event_time_secs=0)
              .advance_processing_time(1)
              .add_element(element=1, event_time_secs=1)
              .advance_processing_time(1)
              .add_element(element=2, event_time_secs=3)
              .advance_processing_time(1)
              .add_element(element=3, event_time_secs=4)
              .advance_processing_time(1)
              .add_element(element=4, event_time_secs=5)
              .build()) # yapf: disable

    self.mock_result.set_state(PipelineState.DONE)
    self.cache.write(values, 'full', self.cache_key)
    self.cache.save_pcoder(None, 'full', self.cache_key)

    # The elements read from the cache are TestStreamFileRecord instances and
    # have the underlying elements encoded. This method decodes the elements
    # from the TestStreamFileRecord.
    def get_elements(events):
      coder = coders.FastPrimitivesCoder()
      elements = []
      for e in events:
        if not isinstance(e, TestStreamFileRecord):
          continue

        if e.recorded_event.element_event:
          elements += ([
              coder.decode(el.encoded_element)
              for el in e.recorded_event.element_event.elements
          ])
      return elements

    # The following tests a progression of reading different durations from the
    # cache.
    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=1)
    self.assertSequenceEqual(get_elements(stream.read()), [0])

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=2)
    self.assertSequenceEqual(get_elements(stream.read()), [0, 1])

    stream = ElementStream(
        self.pcoll, '', self.cache_key, max_n=100, max_duration_secs=10)
    self.assertSequenceEqual(get_elements(stream.read()), [0, 1, 2, 3, 4])


class RecordingTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @unittest.skipIf(
      sys.version_info < (3, 5, 3),
      'This test requires at least Python 3.6 to work.')
  def test_computed(self):
    """Tests that a PCollection is marked as computed only in a complete state.

    Because the background caching job is now long-lived, repeated runs of a
    PipelineFragment may yield different results for the same PCollection.
    """

    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    ib.watch(locals())

    # Create a MockPipelineResult to control the state of a fake run of the
    # pipeline.
    mock_result = MockPipelineResult()
    ie.current_env().track_user_pipelines()
    ie.current_env().set_pipeline_result(p, mock_result)

    # Create a mock BackgroundCachingJob that will control whether to set the
    # PCollections as computed or not.
    bcj_mock_result = MockPipelineResult()
    background_caching_job = bcj.BackgroundCachingJob(bcj_mock_result, [])

    # Create a recording.
    recording = Recording(
        p, [elems],
        mock_result,
        pi.PipelineInstrument(p),
        max_n=10,
        max_duration_secs=60)

    # The background caching job and the recording isn't done yet so there may
    # be more elements to be recorded.
    self.assertFalse(recording.is_computed())
    self.assertFalse(recording.computed())
    self.assertTrue(recording.uncomputed())

    # The recording is finished but the background caching job is not. There
    # may still be more elements to record, or the intermediate PCollection may
    # have stopped caching in an incomplete state, e.g. before a window could
    # fire.
    mock_result.set_state(PipelineState.DONE)
    recording.wait_until_finish()

    self.assertFalse(recording.is_computed())
    self.assertFalse(recording.computed())
    self.assertTrue(recording.uncomputed())

    # The background caching job finished before we started a recording which
    # is a sure signal that there will be no more elements.
    bcj_mock_result.set_state(PipelineState.DONE)
    ie.current_env().set_background_caching_job(p, background_caching_job)
    recording = Recording(
        p, [elems],
        mock_result,
        pi.PipelineInstrument(p),
        max_n=10,
        max_duration_secs=60)
    recording.wait_until_finish()

    # There are no more elements and the recording finished, meaning that the
    # intermediate PCollections are in a complete state. They can now be marked
    # as computed.
    self.assertTrue(recording.is_computed())
    self.assertTrue(recording.computed())
    self.assertFalse(recording.uncomputed())


class RecordingManagerTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @unittest.skipIf(
      sys.version_info < (3, 5, 3),
      'This test requires at least Python 3.6 to work.')
  def test_basic_wordcount(self):
    """A wordcount to be used as a smoke test."""

    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    rm = RecordingManager(p)

    recording = rm.record([elems], max_n=3, max_duration_secs=500)
    stream = recording.stream(elems)

    recording.wait_until_finish()

    elems = list(stream.read())

    expected_elems = [
        WindowedValue(i, MIN_TIMESTAMP, [GlobalWindow()]) for i in range(3)
    ]
    self.assertListEqual(elems, expected_elems)


if __name__ == '__main__':
  unittest.main()
