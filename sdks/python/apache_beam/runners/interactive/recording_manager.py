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

import logging
import threading
import time
import warnings
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe.frame_base import DeferredBase
from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import runner
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import utils
from apache_beam.runners.interactive.caching.cacheable import CacheKey
from apache_beam.runners.interactive.options import capture_control
from apache_beam.runners.runner import PipelineState

_LOGGER = logging.getLogger(__name__)


class ElementStream:
  """A stream of elements from a given PCollection."""
  def __init__(
      self,
      pcoll: beam.pvalue.PCollection,
      var: str,
      cache_key: str,
      max_n: int,
      max_duration_secs: float):
    self._pcoll = pcoll
    self._cache_key = cache_key
    self._pipeline = ie.current_env().user_pipeline(pcoll.pipeline)
    self._var = var
    self._n = max_n
    self._duration_secs = max_duration_secs

    # A small state variable that when True, indicates that no more new elements
    # will be yielded if read() is called again.
    self._done = False

  @property
  def var(self) -> str:
    """Returns the variable named that defined this PCollection."""
    return self._var

  @property
  def pcoll(self) -> beam.pvalue.PCollection:
    """Returns the PCollection that supplies this stream with data."""
    return self._pcoll

  @property
  def cache_key(self) -> str:
    """Returns the cache key for this stream."""
    return self._cache_key

  def display_id(self, suffix: str) -> str:
    """Returns a unique id able to be displayed in a web browser."""
    return utils.obfuscate(self._cache_key, suffix)

  def is_computed(self) -> bool:
    # noqa: F821

    """Returns True if no more elements will be recorded."""
    return self._pcoll in ie.current_env().computed_pcollections

  def is_done(self) -> bool:
    # noqa: F821

    """Returns True if no more new elements will be yielded."""
    return self._done

  def read(self, tail: bool = True) -> Any:
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
      if isinstance(e, beam_runner_api_pb2.TestStreamPayload.Event):
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
      user_pipeline: beam.Pipeline,
      pcolls: List[beam.pvalue.PCollection], # noqa: F821
      result: 'beam.runner.PipelineResult',
      max_n: int,
      max_duration_secs: float,
      ):
    self._user_pipeline = user_pipeline
    self._result = result
    self._result_lock = threading.Lock()
    self._pcolls = pcolls
    pcoll_var = lambda pcoll: {v: k
                               for k, v in utils.pcoll_by_name().items()}.get(
                                   pcoll, None)

    self._streams = {
        pcoll: ElementStream(
            pcoll,
            pcoll_var(pcoll),
            CacheKey.from_pcoll(pcoll_var(pcoll), pcoll).to_str(),
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

  def _mark_all_computed(self) -> None:
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

  def is_computed(self) -> bool:
    """Returns True if all PCollections are computed."""
    return all(s.is_computed() for s in self._streams.values())

  def stream(self, pcoll: beam.pvalue.PCollection) -> ElementStream:
    """Returns an ElementStream for a given PCollection."""
    return self._streams[pcoll]

  def computed(self) -> None:
    """Returns all computed ElementStreams."""
    return {p: s for p, s in self._streams.items() if s.is_computed()}

  def uncomputed(self) -> None:
    """Returns all uncomputed ElementStreams."""
    return {p: s for p, s in self._streams.items() if not s.is_computed()}

  def cancel(self) -> None:
    """Cancels the recording."""
    with self._result_lock:
      self._result.cancel()

  def wait_until_finish(self) -> None:
    """Waits until the pipeline is done and returns the final state.

    This also marks any PCollections as computed right away if the pipeline is
    successful.
    """
    if not self._result:
      return beam.runners.runner.PipelineState.DONE

    self._mark_computed.join()
    return self._result.state

  def describe(self) -> Dict[str, int]:
    """Returns a dictionary describing the cache and recording."""
    cache_manager = ie.current_env().get_cache_manager(self._user_pipeline)

    size = sum(
        cache_manager.size('full', s.cache_key) for s in self._streams.values())
    return {'size': size, 'duration': self._duration_secs}


class RecordingManager:
  """Manages recordings of PCollections for a given pipeline."""
  def __init__(
      self,
      user_pipeline: beam.Pipeline,
      pipeline_var: str = None,
      test_limiters: List['Limiter'] = None) -> None:  # noqa: F821

    self.user_pipeline: beam.Pipeline = user_pipeline
    self.pipeline_var: str = pipeline_var if pipeline_var else ''
    self._recordings: set[Recording] = set()
    self._start_time_sec: float = 0
    self._test_limiters = test_limiters if test_limiters else []

  def _watch(self, pcolls: List[beam.pvalue.PCollection]) -> None:
    """Watch any pcollections not being watched.

    This allows for the underlying caching layer to identify the PCollection as
    something to be cached.
    """

    watched_pcollections = set()
    watched_dataframes = set()
    for watching in ie.current_env().watching():
      for _, val in watching:
        if isinstance(val, beam.pvalue.PCollection):
          watched_pcollections.add(val)
        elif isinstance(val, DeferredBase):
          watched_dataframes.add(val)

    # Convert them one-by-one to generate a unique label for each. This allows
    # caching at a more fine-grained granularity.
    #
    # TODO(https://github.com/apache/beam/issues/20929): investigate the mixing
    # pcollections in multiple pipelines error when using the default label.
    for df in watched_dataframes:
      pcoll, _ = utils.deferred_df_to_pcollection(df)
      watched_pcollections.add(pcoll)
    for pcoll in pcolls:
      if pcoll not in watched_pcollections:
        ie.current_env().watch(
            {'anonymous_pcollection_{}'.format(id(pcoll)): pcoll})

  def _clear(self) -> None:
    """Clears the recording of all non-source PCollections."""

    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)

    # Only clear the PCollections that aren't being populated from the
    # BackgroundCachingJob.
    computed = ie.current_env().computed_pcollections
    cacheables = [
        c for c in utils.cacheables().values()
        if c.pcoll.pipeline is self.user_pipeline and c.pcoll not in computed
    ]
    all_cached = set(str(c.to_key()) for c in cacheables)
    source_pcolls = getattr(cache_manager, 'capture_keys', set())
    to_clear = all_cached - source_pcolls

    self._clear_pcolls(cache_manager, set(to_clear))

  def _clear_pcolls(self, cache_manager, pcolls):
    for pc in pcolls:
      cache_manager.clear('full', pc)

  def clear(self) -> None:
    """Clears all cached PCollections for this RecordingManager."""
    cache_manager = ie.current_env().get_cache_manager(self.user_pipeline)
    if cache_manager:
      cache_manager.cleanup()

  def cancel(self: None) -> None:
    """Cancels the current background recording job."""

    bcj.attempt_to_cancel_background_caching_job(self.user_pipeline)

    for r in self._recordings:
      r.wait_until_finish()
    self._recordings = set()

    # The recordings rely on a reference to the BCJ to correctly finish. So we
    # evict the BCJ after they complete.
    ie.current_env().evict_background_caching_job(self.user_pipeline)

  def describe(self) -> Dict[str, int]:
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

  def record_pipeline(self) -> bool:
    """Starts a background caching job for this RecordingManager's pipeline."""

    runner = self.user_pipeline.runner
    if isinstance(runner, ir.InteractiveRunner):
      runner = runner._underlying_runner

    # Make sure that sources without a user reference are still cached.
    ie.current_env().add_user_pipeline(self.user_pipeline)
    utils.watch_sources(self.user_pipeline)

    # Attempt to run background caching job to record any sources.
    warnings.filterwarnings(
        'ignore',
        'options is deprecated since First stable release. References to '
        '<pipeline>.options will not be supported',
        category=DeprecationWarning)
    if bcj.attempt_to_run_background_caching_job(
        runner,
        self.user_pipeline,
        options=self.user_pipeline.options,
        limiters=self._test_limiters):
      self._start_time_sec = time.time()
      return True
    return False

  def record(
      self,
      pcolls: List[beam.pvalue.PCollection],
      *,
      max_n: int,
      max_duration: Union[int, str],
      runner: runner.PipelineRunner = None,
      options: pipeline_options.PipelineOptions = None,
      force_compute: bool = False) -> Recording:
    # noqa: F821

    """Records the given PCollections."""

    if not ie.current_env().options.enable_recording_replay:
      capture_control.evict_captured_data()
    if force_compute:
      ie.current_env().evict_computed_pcollections()

    # Assert that all PCollection come from the same user_pipeline.
    for pcoll in pcolls:
      assert pcoll.pipeline is self.user_pipeline, (
        '{} belongs to a different user-defined pipeline ({}) than that of'
        ' other PCollections ({}).'.format(
            pcoll, pcoll.pipeline, self.user_pipeline))

    if isinstance(max_duration, str) and max_duration != 'inf':
      max_duration_secs = pd.to_timedelta(max_duration).total_seconds()
    else:
      max_duration_secs = max_duration

    # Make sure that all PCollections to be shown are watched. If a PCollection
    # has not been watched, make up a variable name for that PCollection and
    # watch it. No validation is needed here because the watch logic can handle
    # arbitrary variables.
    self._watch(pcolls)
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
      self._clear()

      merged_options = pipeline_options.PipelineOptions(
          **{
              **self.user_pipeline.options.get_all_options(
                  drop_default=True, retain_unknown_options=True),
              **options.get_all_options(
                  drop_default=True, retain_unknown_options=True)
          }) if options else self.user_pipeline.options

      cache_path = ie.current_env().options.cache_root
      is_remote_run = cache_path and ie.current_env(
      ).options.cache_root.startswith('gs://')
      pf.PipelineFragment(
          list(uncomputed_pcolls), merged_options,
          runner=runner).run(blocking=is_remote_run)
      result = ie.current_env().pipeline_result(self.user_pipeline)
    else:
      result = None

    recording = Recording(
        self.user_pipeline, pcolls, result, max_n, max_duration_secs)
    self._recordings.add(recording)

    return recording

  def read(
      self,
      pcoll_name: str,
      pcoll: beam.pvalue.PValue,
      max_n: int,
      max_duration_secs: float) -> Union[None, ElementStream]:
    # noqa: F821

    """Reads an ElementStream of a computed PCollection.

    Returns None if an error occurs. The caller is responsible of validating if
    the given pcoll_name and pcoll can identify a watched and computed
    PCollection without ambiguity in the notebook.
    """

    try:
      cache_key = CacheKey.from_pcoll(pcoll_name, pcoll).to_str()
      return ElementStream(
          pcoll, pcoll_name, cache_key, max_n, max_duration_secs)
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      # Caller should handle all validations. Here to avoid redundant
      # validations, simply log errors if caller fails to do so.
      _LOGGER.error(str(e))
      return None
