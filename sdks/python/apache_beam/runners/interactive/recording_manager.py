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
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import pipeline_instrument as pi
from apache_beam.runners.interactive import utils
from apache_beam.runners.interactive.options.capture_limiters import CountLimiter
from apache_beam.runners.interactive.options.capture_limiters import ProcessingTimeLimiter

_LOGGER = logging.getLogger(__name__)

PipelineState = beam.runners.runner.PipelineState


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

  def var(self):
    # type: () -> str

    """Returns the variable named that defined this PCollection."""
    return self._var

  def display_id(self, suffix):
    #Any type: (str) -> str

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
    while not cache_manager.exists('full', self._cache_key):
      pass

    # Retrieve the coder for the particular PCollection which will be used to
    # decode elements read from cache.
    coder = cache_manager.load_pcoder('full', self._cache_key)

    # Read the elements from the cache.
    limiters = [
        CountLimiter(self._n), ProcessingTimeLimiter(self._duration_secs)
    ]
    if hasattr(cache_manager, 'read_multiple'):
      reader = cache_manager.read_multiple([('full', self._cache_key)],
                                           limiters=limiters,
                                           tail=tail)
    else:
      reader, _ = cache_manager.read('full', self._cache_key, limiters=limiters)

    # Because a single TestStreamFileRecord can yield multiple elements, we
    # limit the count again here in the to_element_list call.
    for e in utils.to_element_list(reader,
                                   coder,
                                   include_window_info=True,
                                   n=self._n):
      yield e

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
      max_duration_secs  # type: float
      ):

    self._user_pipeline = user_pipeline
    self._result = result
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
      if time.time() - self._start >= self._duration_secs:
        self._result.cancel()
        self._result.wait_until_finish()

      if all(s.is_done() for s in self._streams.values()):
        self._result.cancel()
        self._result.wait_until_finish()

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


class RecordingManager:
  """Manages recordings of PCollections for a given pipeline."""
  def __init__(self, user_pipeline):
    # type: (beam.Pipeline, List[Limiter]) -> None
    self.user_pipeline = user_pipeline
    self._pipeline_instrument = pi.PipelineInstrument(self.user_pipeline)

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

  def clear(self, pcolls):
    # type: (List[beam.pvalue.PCollection]) -> None

    """Clears the cache of the given PCollections."""

    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)
    for pc in pcolls:
      cache_key = self._pipeline_instrument.cache_key(pc)
      cache_manager.clear('full', cache_key)

  def record(self, pcolls, max_n, max_duration_secs):
    # type: (List[beam.pvalue.PCollection], int, int) -> Recording

    """Records the given PCollections."""

    # Assert that all PCollection come from the same user_pipeline.
    for pcoll in pcolls:
      assert pcoll.pipeline is self.user_pipeline, (
        '{} belongs to a different user-defined pipeline ({}) than that of'
        ' other PCollections ({}).'.format(
            pcoll, pcoll.pipeline, self.user_pipeline))

    runner = self.user_pipeline.runner
    if isinstance(runner, ir.InteractiveRunner):
      runner = runner._underlying_runner

    # Make sure that sources without a user reference are still cached.
    pi.watch_sources(self.user_pipeline)

    # Make sure that all PCollections to be shown are watched. If a PCollection
    # has not been watched, make up a variable name for that PCollection and
    # watch it. No validation is needed here because the watch logic can handle
    # arbitrary variables.
    self._watch(pcolls)

    # Attempt to run background caching job to record any sources.
    if ie.current_env().is_in_ipython:
      warnings.filterwarnings(
          'ignore',
          'options is deprecated since First stable release. References to '
          '<pipeline>.options will not be supported',
          category=DeprecationWarning)
    bcj.attempt_to_run_background_caching_job(
        runner, self.user_pipeline, options=self.user_pipeline.options)

    # Get the subset of computed PCollections. These do not to be recomputed.
    computed_pcolls = set(
        pcoll for pcoll in pcolls
        if pcoll in ie.current_env().computed_pcollections)

    # Start a pipeline fragment to start computing the PCollections.
    uncomputed_pcolls = set(pcolls).difference(computed_pcolls)
    if uncomputed_pcolls:
      # Clear the cache of the given uncomputed PCollections because they are
      # incomplete.
      self.clear(uncomputed_pcolls)

      warnings.filterwarnings(
          'ignore',
          'options is deprecated since First stable release. References to '
          '<pipeline>.options will not be supported',
          category=DeprecationWarning)
      result = pf.PipelineFragment(
          list(uncomputed_pcolls), self.user_pipeline.options).run()
      ie.current_env().set_pipeline_result(self.user_pipeline, result)
    else:
      result = None

    return Recording(
        self.user_pipeline,
        pcolls,
        result,
        self._pipeline_instrument,
        max_n,
        max_duration_secs)
