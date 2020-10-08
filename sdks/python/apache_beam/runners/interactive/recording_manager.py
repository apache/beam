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

from __future__ import absolute_import

import logging
import threading
import time
import warnings

import apache_beam as beam
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive import utils
from apache_beam.runners.runner import PipelineState

_LOGGER = logging.getLogger(__name__)


class ElementStream:
  """A stream of elements from a given PCollection."""
  def __init__(
      self,
      pcoll,  # type: beam.pvalue.PCollection
      var,  # type: str
      cache_key,  # type: str
      max_n,  # type: int
      max_duration_secs  # type: float
      ):
    self._pcoll = pcoll
    self._cache_key = cache_key
    self._pipeline = pcoll.pipeline
    self._var = var
    self._n = max_n
    self._duration_secs = max_duration_secs

    # A small state variable that when True, indicates that no more new elements
    # will be yielded if read() is called again.
    self._done = False

  @property
  def var(self):
    # type: () -> str

    """Returns the variable named that defined this PCollection."""
    return self._var

  @property
  def cache_key(self):
    # type: () -> str

    """Returns the cache key for this stream."""
    return self._cache_key

  def display_id(self, suffix):
    # type: (str) -> str

    """Returns a unique id able to be displayed in a web browser."""
    return utils.obfuscate(self._cache_key, suffix)

  def is_computed(self):
    # type: () -> boolean

    """Returns True if no more elements will be recorded."""
    return self._pcoll in ie.current_env().computed_pcollections

  def is_done(self):
    # type: () -> boolean

    """Returns True if no more new elements will be yielded."""
    return self._done

  def read(self, tail=True):
    # type: (boolean) -> Any

    """Reads the elements currently recorded."""

    # Get the cache manager and wait until the file exists.
    cache_manager = ie.current_env().get_cache_manager(self._pipeline)

    # Retrieve the coder for the particular PCollection which will be used to
    # decode elements read from cache.
    coder = cache_manager.load_pcoder('full', self._cache_key)

    # Read the elements from the cache.
    # Import limiters here to prevent a circular import.
    from apache_beam.runners.interactive.options.capture_limiters import CountLimiter
    from apache_beam.runners.interactive.options.capture_limiters import ProcessingTimeLimiter
    reader, _ = cache_manager.read('full', self._cache_key, tail=tail)

    # Because a single TestStreamFileRecord can yield multiple elements, we
    # limit the count again here in the to_element_list call.
    #
    # There are two ways of exiting this loop either a limiter was triggered or
    # all elements from the cache were read. In the latter situation, it may be
    # the case that the pipeline was still running. Thus, another invocation of
    # `read` will yield new elements.
    count_limiter = CountLimiter(self._n)
    time_limiter = ProcessingTimeLimiter(self._duration_secs)
    limiters = (count_limiter, time_limiter)
    for e in utils.to_element_list(reader,
                                   coder,
                                   include_window_info=True,
                                   n=self._n,
                                   include_time_events=True):

      # From the to_element_list we either get TestStreamPayload.Events if
      # include_time_events or decoded elements from the reader. Make sure we
      # only count the decoded elements to break early.
      if isinstance(e, TestStreamPayload.Event):
        time_limiter.update(e)
      else:
        count_limiter.update(e)
        yield e

      if any(l.is_triggered() for l in limiters):
        break

    # A limiter being triggered means that we have fulfilled the user's request.
    # This implies that reading from the cache again won't yield any new
    # elements. WLOG, this applies to the user pipeline being terminated.
    if any(l.is_triggered()
           for l in limiters) or ie.current_env().is_terminated(self._pipeline):
      self._done = True


class Recording:
  """A group of PCollections from a given pipeline run."""
  def __init__(
      self,
      user_pipeline,  # type: beam.Pipeline
      pcolls,  # type: List[beam.pvalue.PCollection]
      result,  # type: beam.runner.PipelineResult
      pipeline_instrument,  # type: beam.runners.interactive.PipelineInstrument
      max_n,  # type: int
      max_duration_secs,  # type: float
      ):

    self._user_pipeline = user_pipeline
    self._result = result
    self._result_lock = threading.Lock()
    self._pcolls = pcolls

    pcoll_var = lambda pcoll: pipeline_instrument.cacheable_var_by_pcoll_id(
        pipeline_instrument.pcolls_to_pcoll_id.get(str(pcoll), None))

    self._streams = {
        pcoll: ElementStream(
            pcoll,
            pcoll_var(pcoll),
            pipeline_instrument.cache_key(pcoll),
            max_n,
            max_duration_secs)
        for pcoll in pcolls
    }

    self._start = time.time()
    self._duration_secs = max_duration_secs
    self._set_computed = bcj.is_cache_complete(str(id(user_pipeline)))

    # Run a separate thread for marking the PCollections done. This is because
    # the pipeline run may be asynchronous.
    self._mark_computed = threading.Thread(target=self._mark_all_computed)
    self._mark_computed.daemon = True
    self._mark_computed.start()

  def _mark_all_computed(self):
    # type: () -> None

    """Marks all the PCollections upon a successful pipeline run."""
    if not self._result:
      return

    while not PipelineState.is_terminal(self._result.state):
      with self._result_lock:
        bcj = ie.current_env().get_background_caching_job(self._user_pipeline)
        if bcj and bcj.is_done():
          self._result.wait_until_finish()

        elif time.time() - self._start >= self._duration_secs:
          self._result.cancel()
          self._result.wait_until_finish()

        elif all(s.is_done() for s in self._streams.values()):
          self._result.cancel()
          self._result.wait_until_finish()

      time.sleep(0.1)

    # Mark the PCollection as computed so that Interactive Beam wouldn't need to
    # re-compute.
    if self._result.state is PipelineState.DONE and self._set_computed:
      ie.current_env().mark_pcollection_computed(self._pcolls)

  def is_computed(self):
    # type: () -> boolean

    """Returns True if all PCollections are computed."""
    return all(s.is_computed() for s in self._streams.values())

  def stream(self, pcoll):
    # type: (beam.pvalue.PCollection) -> ElementStream

    """Returns an ElementStream for a given PCollection."""
    return self._streams[pcoll]

  def computed(self):
    # type: () -> None

    """Returns all computed ElementStreams."""
    return {p: s for p, s in self._streams.items() if s.is_computed()}

  def uncomputed(self):
    # type: () -> None

    """Returns all uncomputed ElementStreams."""
    return {p: s for p, s in self._streams.items() if not s.is_computed()}

  def cancel(self):
    # type: () -> None

    """Cancels the recording."""
    with self._result_lock:
      self._result.cancel()

  def wait_until_finish(self):
    # type: () -> None

    """Waits until the pipeline is done and returns the final state.

    This also marks any PCollections as computed right away if the pipeline is
    successful.
    """
    if not self._result:
      return beam.runners.runner.PipelineState.DONE

    self._mark_computed.join()
    return self._result.state

  def describe(self):
    # type: () -> dict[str, int]

    """Returns a dictionary describing the cache and recording."""
    cache_manager = ie.current_env().get_cache_manager(self._user_pipeline)

    size = sum(
        cache_manager.size('full', s.cache_key) for s in self._streams.values())
    return {'size': size}


class RecordingManager:
  """Manages recordings of PCollections for a given pipeline."""
  def __init__(self, user_pipeline, pipeline_var=None):
    # type: (beam.Pipeline, str) -> None

    self.user_pipeline = user_pipeline  # type: beam.Pipeline
    self.pipeline_var = pipeline_var if pipeline_var else ''  # type: str
    self._recordings = set()  # type: set[Recording]
    self._start_time_sec = 0  # type: float

  def _watch(self, pcolls):
    # type: (List[beam.pvalue.PCollection]) -> None

    """Watch any pcollections not being watched.

    This allows for the underlying caching layer to identify the PCollection as
    something to be cached.
    """

    watched_pcollections = set()
    for watching in ie.current_env().watching():
      for _, val in watching:
        if isinstance(val, beam.pvalue.PCollection):
          watched_pcollections.add(val)
    for pcoll in pcolls:
      if pcoll not in watched_pcollections:
        ie.current_env().watch(
            {'anonymous_pcollection_{}'.format(id(pcoll)): pcoll})

  def _clear(self, pipeline_instrument):
    # type: (List[beam.pvalue.PCollection]) -> None

    """Clears the recording of all non-source PCollections."""

    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)

    # Only clear the PCollections that aren't being populated from the
    # BackgroundCachingJob.
    computed = ie.current_env().computed_pcollections
    cacheables = [
        c for c in pipeline_instrument.cacheables.values()
        if c.pcoll.pipeline is self.user_pipeline and c.pcoll not in computed
    ]
    all_cached = set(str(c.to_key()) for c in cacheables)
    source_pcolls = getattr(cache_manager, 'capture_keys', set())
    to_clear = all_cached - source_pcolls

    for cache_key in to_clear:
      cache_manager.clear('full', cache_key)

  def clear(self):
    # type: () -> None

    """Clears all cached PCollections for this RecordingManager."""
    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)
    if cache_manager:
      cache_manager.cleanup()

  def cancel(self):
    # type: (None) -> None

    """Cancels the current background recording job."""

    bcj.attempt_to_cancel_background_caching_job(self.user_pipeline)

    for r in self._recordings:
      r.wait_until_finish()
    self._recordings = set()

    # The recordings rely on a reference to the BCJ to correctly finish. So we
    # evict the BCJ after they complete.
    ie.current_env().evict_background_caching_job(self.user_pipeline)

  def describe(self):
    # type: () -> dict[str, int]

    """Returns a dictionary describing the cache and recording."""

    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)
    capture_size = getattr(cache_manager, 'capture_size', 0)

    descriptions = [r.describe() for r in self._recordings]
    size = sum(d['size'] for d in descriptions) + capture_size
    start = self._start_time_sec
    bcj = ie.current_env().get_background_caching_job(self.user_pipeline)
    if bcj:
      state = bcj.state
    else:
      state = PipelineState.STOPPED
    return {
        'size': size,
        'start': start,
        'state': state,
        'pipeline_var': self.pipeline_var
    }

  def record_pipeline(self):
    # type: () -> bool

    """Starts a background caching job for this RecordingManager's pipeline."""

    runner = self.user_pipeline.runner
    if isinstance(runner, ir.InteractiveRunner):
      runner = runner._underlying_runner

    # Make sure that sources without a user reference are still cached.
    pi.watch_sources(self.user_pipeline)

    # Attempt to run background caching job to record any sources.
    if ie.current_env().is_in_ipython:
      warnings.filterwarnings(
          'ignore',
          'options is deprecated since First stable release. References to '
          '<pipeline>.options will not be supported',
          category=DeprecationWarning)
    if bcj.attempt_to_run_background_caching_job(
        runner, self.user_pipeline, options=self.user_pipeline.options):
      self._start_time_sec = time.time()
      return True
    return False

  def record(self, pcolls, max_n, max_duration_secs):
    # type: (List[beam.pvalue.PCollection], int, int) -> Recording

    """Records the given PCollections."""

    # Assert that all PCollection come from the same user_pipeline.
    for pcoll in pcolls:
      assert pcoll.pipeline is self.user_pipeline, (
        '{} belongs to a different user-defined pipeline ({}) than that of'
        ' other PCollections ({}).'.format(
            pcoll, pcoll.pipeline, self.user_pipeline))

    # Make sure that all PCollections to be shown are watched. If a PCollection
    # has not been watched, make up a variable name for that PCollection and
    # watch it. No validation is needed here because the watch logic can handle
    # arbitrary variables.
    self._watch(pcolls)
    pipeline_instrument = pi.PipelineInstrument(self.user_pipeline)

    pipeline_instrument = pi.PipelineInstrument(self.user_pipeline)
    self.record_pipeline()

    # Get the subset of computed PCollections. These do not to be recomputed.
    computed_pcolls = set(
        pcoll for pcoll in pcolls
        if pcoll in ie.current_env().computed_pcollections)

    # Start a pipeline fragment to start computing the PCollections.
    uncomputed_pcolls = set(pcolls).difference(computed_pcolls)
    if uncomputed_pcolls:
      # Clear the cache of the given uncomputed PCollections because they are
      # incomplete.
      self._clear(pipeline_instrument)

      warnings.filterwarnings(
          'ignore',
          'options is deprecated since First stable release. References to '
          '<pipeline>.options will not be supported',
          category=DeprecationWarning)
      pf.PipelineFragment(list(uncomputed_pcolls),
                          self.user_pipeline.options).run()
      result = ie.current_env().pipeline_result(self.user_pipeline)
    else:
      result = None

    recording = Recording(
        self.user_pipeline,
        pcolls,
        result,
        pipeline_instrument,
        max_n,
        max_duration_secs)
    self._recordings.add(recording)

    return recording
