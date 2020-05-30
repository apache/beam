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

import unittest

import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.runners.interactive.recording_manager import ElementStream
from apache_beam.runners.interactive.recording_manager import Recording
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.windowed_value import WindowedValue

PipelineState = beam.runners.runner.PipelineState


class ElementStreamTest(unittest.TestCase):
  def test_read(self):
    cache = InMemoryCache()
    ie.current_env().set_cache_manager(cache)

    cache_key = str(pi.CacheKey('elem', '', '', ''))
    stream = ElementStream(None, cache_key)

    cache.write(['expected'], 'full', cache_key)
    cache.save_pcoder(None, 'full', cache_key)

    self.assertEqual(list(stream.read())[0], 'expected')
    ie.current_env().set_cache_manager(None)

  def test_var_name(self):
    stream = ElementStream(None, str(pi.CacheKey('elem', '', '', '')))
    self.assertEqual(stream.var(), 'elem')


class RecordingTest(unittest.TestCase):
  def test_computed(self):
    class MockPipelineResult(beam.runners.runner.PipelineResult):
      def __init__(self):
        self._state = PipelineState.RUNNING

      def wait_until_finish(self):
        pass

      def set_state(self, state):
        self._state = state

      @property
      def state(self):
        return self._state

    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    ib.watch(locals())

    mock_result = MockPipelineResult()
    ie.current_env().set_pipeline_result(p, mock_result)

    recording = Recording([elems], mock_result, pi.PipelineInstrument(p))

    stream = recording.stream(elems)

    self.assertFalse(recording.is_computed())
    self.assertFalse(recording.computed())
    self.assertTrue(recording.uncomputed())

    mock_result.set_state(PipelineState.DONE)
    recording.wait_until_finish()

    self.assertTrue(recording.is_computed())
    self.assertTrue(recording.computed())
    self.assertFalse(recording.uncomputed())


class RecordingManagerTest(unittest.TestCase):
  def test_basic_wordcount(self):
    p = beam.Pipeline(InteractiveRunner())
    elems = p | beam.Create([0, 1, 2])

    ib.watch(locals())

    rm = RecordingManager(p)

    recording = rm.record_all([elems])
    stream = recording.stream(elems)

    recording.wait_until_finish()

    elems = list(stream.read())

    expected_elems = [
        WindowedValue(i, MIN_TIMESTAMP, [GlobalWindow()]) for i in range(3)
    ]
    self.assertListEqual(elems, expected_elems)

  def test_watch_for_anonymous_pcollections(self):
    p = beam.Pipeline(InteractiveRunner())
    ib.watch(locals())

    # Because this PCollection is after the ib.watch(), it isn't being watched
    # by the environment. It will be added by the RecordingManager. This can
    # happen if the user performs an ib.show() on an expression.
    elems = p | beam.Create([0, 1, 2])

    rm = RecordingManager(p)

    recording = rm.record_all([elems])
    stream = recording.stream(elems)

    elems = list(stream.read())

    expected_elems = [
        WindowedValue(i, MIN_TIMESTAMP, [GlobalWindow()]) for i in range(3)
    ]
    self.assertListEqual(elems, expected_elems)


if __name__ == '__main__':
  unittest.main()
