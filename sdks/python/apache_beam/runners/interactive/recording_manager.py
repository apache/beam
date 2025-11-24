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

import collections
import logging
import os
import threading
import time
import uuid
import warnings
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Optional
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
from apache_beam.runners.interactive.display.pipeline_graph import PipelineGraph
from apache_beam.runners.interactive.options import capture_control
from apache_beam.runners.runner import PipelineState

_LOGGER = logging.getLogger(__name__)

try:
  import ipywidgets as widgets
  from IPython.display import HTML
  from IPython.display import clear_output
  from IPython.display import display

  IS_IPYTHON = True
except ImportError:
  IS_IPYTHON = False


class AsyncComputationResult:
  """Represents the result of an asynchronous computation."""
  def __init__(
      self,
      future: Future,
      pcolls: set[beam.pvalue.PCollection],
      user_pipeline: beam.Pipeline,
      recording_manager: 'RecordingManager',
  ):
    self._future = future
    self._pcolls = pcolls
    self._user_pipeline = user_pipeline
    self._env = ie.current_env()
    self._recording_manager = recording_manager
    self._pipeline_result: Optional[beam.runners.runner.PipelineResult] = None
    self._display_id = str(uuid.uuid4())
    self._output_widget = widgets.Output() if IS_IPYTHON else None
    self._cancel_button = (
        widgets.Button(description='Cancel') if IS_IPYTHON else None)
    self._progress_bar = (
        widgets.FloatProgress(
            value=0.0,
            min=0.0,
            max=1.0,
            description='Running:',
            bar_style='info',
        ) if IS_IPYTHON else None)
    self._cancel_requested = False

    if IS_IPYTHON:
      self._cancel_button.on_click(self._cancel_clicked)
      controls = widgets.VBox([
          widgets.HBox([self._cancel_button, self._progress_bar]),
          self._output_widget,
      ])
      display(controls, display_id=self._display_id)
      self.update_display('Initializing...')

    self._future.add_done_callback(self._on_done)

  def _cancel_clicked(self, b):
    self._cancel_requested = True
    self._cancel_button.disabled = True
    self.update_display('Cancel requested...')
    self.cancel()

  def update_display(self, msg: str, progress: Optional[float] = None):
    if not IS_IPYTHON:
      print(f'AsyncCompute: {msg}')
      return

    with self._output_widget:
      clear_output(wait=True)
      display(HTML(f'<p>{msg}</p>'))

    if progress is not None:
      self._progress_bar.value = progress

    if self.done():
      self._cancel_button.disabled = True
      if self.exception():
        self._progress_bar.bar_style = 'danger'
        self._progress_bar.description = 'Failed'
      elif self._future.cancelled():
        self._progress_bar.bar_style = 'warning'
        self._progress_bar.description = 'Cancelled'
      else:
        self._progress_bar.bar_style = 'success'
        self._progress_bar.description = 'Done'
    elif self._cancel_requested:
      self._cancel_button.disabled = True
      self._progress_bar.description = 'Cancelling...'
    else:
      self._cancel_button.disabled = False

  def set_pipeline_result(
      self, pipeline_result: beam.runners.runner.PipelineResult):
    self._pipeline_result = pipeline_result
    if self._cancel_requested:
      self.cancel()

  def result(self, timeout=None):
    return self._future.result(timeout=timeout)

  def done(self):
    return self._future.done()

  def exception(self, timeout=None):
    try:
      return self._future.exception(timeout=timeout)
    except TimeoutError:
      return None

  def _on_done(self, future: Future):
    self._env.unmark_pcollection_computing(self._pcolls)
    self._recording_manager._async_computations.pop(self._display_id, None)

    if future.cancelled():
      self.update_display('Computation Cancelled.', 1.0)
      return

    exc = future.exception()
    if exc:
      self.update_display(f'Error: {exc}', 1.0)
      _LOGGER.error('Asynchronous computation failed: %s', exc, exc_info=exc)
    else:
      self.update_display('Computation Finished Successfully.', 1.0)
      res = future.result()
      if res and res.state == PipelineState.DONE:
        self._env.mark_pcollection_computed(self._pcolls)
      else:
        _LOGGER.warning(
            'Async computation finished but state is not DONE: %s',
            res.state if res else 'Unknown')

  def cancel(self):
    if self._future.done():
      self.update_display('Cannot cancel: Computation already finished.')
      return False

    self._cancel_requested = True
    self._cancel_button.disabled = True
    self.update_display('Attempting to cancel...')

    if self._pipeline_result:
      try:
        # Check pipeline state before cancelling
        current_state = self._pipeline_result.state
        if PipelineState.is_terminal(current_state):
          self.update_display(
              'Cannot cancel: Pipeline already in terminal state'
              f' {current_state}.')
          return False

        self._pipeline_result.cancel()
        self.update_display('Cancel signal sent to pipeline.')
        # The future will be cancelled by the runner if successful
        return True
      except Exception as e:
        self.update_display('Error sending cancel signal: %s', e)
        _LOGGER.warning('Error during pipeline cancel(): %s', e, exc_info=e)
        # Still try to cancel the future as a fallback
        return self._future.cancel()
    else:
      self.update_display('Pipeline not yet fully started, cancelling future.')
      return self._future.cancel()

  def __repr__(self):
    return (
        f'<AsyncComputationResult({self._display_id}) for'
        f' {len(self._pcolls)} PCollections, status:'
        f" {'done' if self.done() else 'running'}>")


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
      pcolls: list[beam.pvalue.PCollection],  # noqa: F821
      result: 'beam.runner.PipelineResult',
      max_n: int,
      max_duration_secs: float,
  ):
    self._user_pipeline = user_pipeline
    self._result = result
    self._result_lock = threading.Lock()
    self._pcolls = pcolls
    pcoll_var = lambda pcoll: {
        v: k
        for k, v in utils.pcoll_by_name().items()
    }.get(pcoll, None)

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

  def describe(self) -> dict[str, int]:
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
      test_limiters: list['Limiter'] = None) -> None:  # noqa: F821

    self.user_pipeline: beam.Pipeline = user_pipeline
    self.pipeline_var: str = pipeline_var if pipeline_var else ''
    self._recordings: set[Recording] = set()
    self._start_time_sec: float = 0
    self._test_limiters = test_limiters if test_limiters else []
    self._executor = ThreadPoolExecutor(max_workers=os.cpu_count())
    self._env = ie.current_env()
    self._async_computations: dict[str, AsyncComputationResult] = {}
    self._pipeline_graph = None

  def _execute_pipeline_fragment(
      self,
      pcolls_to_compute: set[beam.pvalue.PCollection],
      async_result: Optional['AsyncComputationResult'] = None,
      runner: runner.PipelineRunner = None,
      options: pipeline_options.PipelineOptions = None,
  ) -> beam.runners.runner.PipelineResult:
    """Synchronously executes a pipeline fragment for the given PCollections."""
    merged_options = pipeline_options.PipelineOptions(**{
        **self.user_pipeline.options.get_all_options(
            drop_default=True, retain_unknown_options=True
        ),
        **(
            options.get_all_options(
                drop_default=True, retain_unknown_options=True
            )
            if options
            else {}
        ),
    })

    fragment = pf.PipelineFragment(
        list(pcolls_to_compute), merged_options, runner=runner)

    if async_result:
      async_result.update_display('Building pipeline fragment...', 0.1)

    pipeline_to_run = fragment.deduce_fragment()
    if async_result:
      async_result.update_display('"Pipeline running, awaiting finish..."', 0.2)

    pipeline_result = pipeline_to_run.run()
    if async_result:
      async_result.set_pipeline_result(pipeline_result)

    pipeline_result.wait_until_finish()
    return pipeline_result

  def _run_async_computation(
      self,
      pcolls_to_compute: set[beam.pvalue.PCollection],
      async_result: 'AsyncComputationResult',
      wait_for_inputs: bool,
      runner: runner.PipelineRunner = None,
      options: pipeline_options.PipelineOptions = None,
  ):
    """The function to be run in the thread pool for async computation."""
    try:
      if wait_for_inputs:
        if not self._wait_for_dependencies(pcolls_to_compute, async_result):
          raise RuntimeError('Dependency computation failed or was cancelled.')

      _LOGGER.info(
          'Starting asynchronous computation for %d PCollections.',
          len(pcolls_to_compute))

      pipeline_result = self._execute_pipeline_fragment(
          pcolls_to_compute, async_result, runner, options)

      # if pipeline_result.state == PipelineState.DONE:
      #   self._env.mark_pcollection_computed(pcolls_to_compute)
      #   _LOGGER.info(
      #       'Asynchronous computation finished successfully for'
      #       f' {len(pcolls_to_compute)} PCollections.'
      #   )
      # else:
      #   _LOGGER.error(
      #       'Asynchronous computation failed for'
      #       f' {len(pcolls_to_compute)} PCollections. State:'
      #       f' {pipeline_result.state}'
      #   )
      return pipeline_result
    except Exception as e:
      _LOGGER.exception('Exception during asynchronous computation: %s', e)
      raise
    # finally:
    #   self._env.unmark_pcollection_computing(pcolls_to_compute)

  def _watch(self, pcolls: list[beam.pvalue.PCollection]) -> None:
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

  def describe(self) -> dict[str, int]:
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
    if hasattr(runner, 'is_interactive'):
      runner.is_interactive()

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

  def compute_async(
      self,
      pcolls: set[beam.pvalue.PCollection],
      wait_for_inputs: bool = True,
      blocking: bool = False,
      runner: runner.PipelineRunner = None,
      options: pipeline_options.PipelineOptions = None,
      force_compute: bool = False,
  ) -> Optional[AsyncComputationResult]:
    """Computes the given PCollections, potentially asynchronously."""

    if force_compute:
      self._env.evict_computed_pcollections(self.user_pipeline)

    computed_pcolls = {
        pcoll
        for pcoll in pcolls if pcoll in self._env.computed_pcollections
    }
    computing_pcolls = {
        pcoll
        for pcoll in pcolls if self._env.is_pcollection_computing(pcoll)
    }
    pcolls_to_compute = pcolls - computed_pcolls - computing_pcolls

    if not pcolls_to_compute:
      _LOGGER.info(
          'All requested PCollections are already computed or are being'
          ' computed.')
      return None

    self._watch(list(pcolls_to_compute))
    self.record_pipeline()

    if blocking:
      self._env.mark_pcollection_computing(pcolls_to_compute)
      try:
        if wait_for_inputs:
          if not self._wait_for_dependencies(pcolls_to_compute):
            raise RuntimeError(
                'Dependency computation failed or was cancelled.')
        pipeline_result = self._execute_pipeline_fragment(
            pcolls_to_compute, None, runner, options)
        if pipeline_result.state == PipelineState.DONE:
          self._env.mark_pcollection_computed(pcolls_to_compute)
        else:
          _LOGGER.error(
              'Blocking computation failed. State: %s', pipeline_result.state)
          raise RuntimeError(
              'Blocking computation failed. State: %s', pipeline_result.state)
      finally:
        self._env.unmark_pcollection_computing(pcolls_to_compute)
      return None

    else:  # Asynchronous
      future = Future()
      async_result = AsyncComputationResult(
          future, pcolls_to_compute, self.user_pipeline, self)
      self._async_computations[async_result._display_id] = async_result
      self._env.mark_pcollection_computing(pcolls_to_compute)

      def task():
        try:
          result = self._run_async_computation(
              pcolls_to_compute, async_result, wait_for_inputs, runner, options)
          future.set_result(result)
        except Exception as e:
          if not future.cancelled():
            future.set_exception(e)

      self._executor.submit(task)
      return async_result

  def _get_pipeline_graph(self):
    """Lazily initializes and returns the PipelineGraph."""
    if self._pipeline_graph is None:
      try:
        # Try to create the graph.
        self._pipeline_graph = PipelineGraph(self.user_pipeline)
      except (ImportError, NameError, AttributeError):
        # If pydot is missing, PipelineGraph() might crash.
        _LOGGER.warning(
            "Could not create PipelineGraph (pydot missing?). " \
            "Async features disabled."
        )
        self._pipeline_graph = None
    return self._pipeline_graph

  def _get_pcoll_id_map(self):
    """Creates a map from PCollection object to its ID in the proto."""
    pcoll_to_id = {}
    graph = self._get_pipeline_graph()
    if graph and graph._pipeline_instrument:
      pcoll_to_id = graph._pipeline_instrument._pcoll_to_pcoll_id
    return {v: k for k, v in pcoll_to_id.items()}

  def _get_all_dependencies(
      self,
      pcolls: set[beam.pvalue.PCollection]) -> set[beam.pvalue.PCollection]:
    """Gets all upstream PCollection dependencies
    for the given set of PCollections."""
    graph = self._get_pipeline_graph()
    if not graph:
      return set()

    analyzer = graph._pipeline_instrument
    if not analyzer:
      return set()

    pcoll_to_id = analyzer._pcoll_to_pcoll_id

    target_pcoll_ids = {
        pcoll_to_id.get(str(pcoll))
        for pcoll in pcolls if str(pcoll) in pcoll_to_id
    }

    if not target_pcoll_ids:
      return set()

    # Build a map from PCollection ID to the actual PCollection object
    id_to_pcoll_obj = {}
    for _, inspectable in self._env.inspector.inspectables.items():
      value = inspectable['value']
      if isinstance(value, beam.pvalue.PCollection):
        pcoll_id = pcoll_to_id.get(str(value))
        if pcoll_id:
          id_to_pcoll_obj[pcoll_id] = value

    dependencies = set()
    queue = collections.deque(target_pcoll_ids)
    visited_pcoll_ids = set(target_pcoll_ids)

    producers = graph._producers
    transforms = graph._pipeline_proto.components.transforms

    while queue:
      pcoll_id = queue.popleft()
      if pcoll_id not in producers:
        continue

      producer_id = producers[pcoll_id]
      transform_proto = transforms.get(producer_id)
      if not transform_proto:
        continue

      for input_pcoll_id in transform_proto.inputs.values():
        if input_pcoll_id not in visited_pcoll_ids:
          visited_pcoll_ids.add(input_pcoll_id)
          queue.append(input_pcoll_id)

          dep_obj = id_to_pcoll_obj.get(input_pcoll_id)
          if dep_obj and dep_obj not in pcolls:
            dependencies.add(dep_obj)

    return dependencies

  def _wait_for_dependencies(
      self,
      pcolls: set[beam.pvalue.PCollection],
      async_result: Optional[AsyncComputationResult] = None,
  ) -> bool:
    """Waits for any dependencies of the given
    PCollections that are currently being computed."""
    dependencies = self._get_all_dependencies(pcolls)
    computing_deps: dict[beam.pvalue.PCollection, AsyncComputationResult] = {}

    for dep in dependencies:
      if self._env.is_pcollection_computing(dep):
        for comp in self._async_computations.values():
          if dep in comp._pcolls:
            computing_deps[dep] = comp
            break

    if not computing_deps:
      return True

    if async_result:
      async_result.update_display(
          'Waiting for %d dependencies to finish...', len(computing_deps))
    _LOGGER.info(
        'Waiting for %d dependencies: %s',
        len(computing_deps),
        computing_deps.keys())

    futures_to_wait = list(
        set(comp._future for comp in computing_deps.values()))

    try:
      for i, future in enumerate(futures_to_wait):
        if async_result:
          async_result.update_display(
              f'Waiting for dependency {i + 1}/{len(futures_to_wait)}...',
              progress=0.05 + 0.05 * (i / len(futures_to_wait)),
          )
        future.result()
      if async_result:
        async_result.update_display('Dependencies finished.', progress=0.1)
      _LOGGER.info('Dependencies finished successfully.')
      return True
    except Exception as e:
      if async_result:
        async_result.update_display(f'Dependency failed: {e}')
      _LOGGER.error('Dependency computation failed: %s', e, exc_info=e)
      return False

  def record(
      self,
      pcolls: list[beam.pvalue.PCollection],
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
      if not self._wait_for_dependencies(uncomputed_pcolls):
        raise RuntimeError(
            'Cannot record because a dependency failed to compute'
            ' asynchronously.')

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
