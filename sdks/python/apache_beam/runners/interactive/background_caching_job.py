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

"""Module to build and run background source recording jobs.

For internal use only; no backwards-compatibility guarantees.

A background source recording job is a job that records events for all
recordable sources of a given pipeline. With Interactive Beam, one such job is
started when a pipeline run happens (which produces a main job in contrast to
the background source recording job) and meets the following conditions:

  #. The pipeline contains recordable sources, configured through
     interactive_beam.options.recordable_sources.
  #. No such background job is running.
  #. No such background job has completed successfully and the cached events are
     still valid (invalidated when recordable sources change in the pipeline).

Once started, the background source recording job runs asynchronously until it
hits some recording limit configured in interactive_beam.options. Meanwhile,
the main job and future main jobs from the pipeline will run using the
deterministic replayable recorded events until they are invalidated.
"""

# pytype: skip-file

import logging
import threading
import time

import apache_beam as beam
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import utils
from apache_beam.runners.interactive.caching import streaming_cache
from apache_beam.runners.runner import PipelineState

_LOGGER = logging.getLogger(__name__)


class BackgroundCachingJob(object):
  """A simple abstraction that controls necessary components of a timed and
  space limited background source recording job.

  A background source recording job successfully completes source data
  recording in 2 conditions:

    #. The job is finite and runs into DONE state;
    #. The job is infinite but hits an interactive_beam.options configured limit
       and gets cancelled into CANCELLED/CANCELLING state.

  In both situations, the background source recording job should be treated as
  done successfully.
  """
  def __init__(self, pipeline_result, limiters):
    self._pipeline_result = pipeline_result
    self._result_lock = threading.RLock()
    self._condition_checker = threading.Thread(
        target=self._background_caching_job_condition_checker, daemon=True)

    # Limiters are checks s.t. if any are triggered then the background caching
    # job gets cancelled.
    self._limiters = limiters
    self._condition_checker.start()

  def _background_caching_job_condition_checker(self):
    while True:
      with self._result_lock:
        if PipelineState.is_terminal(self._pipeline_result.state):
          break

      if self._should_end_condition_checker():
        self.cancel()
        break
      time.sleep(0.5)

  def _should_end_condition_checker(self):
    return any(l.is_triggered() for l in self._limiters)

  def is_done(self):
    with self._result_lock:
      is_terminated = self._pipeline_result.state in (
          PipelineState.DONE, PipelineState.CANCELLED)
      is_triggered = self._should_end_condition_checker()
      is_cancelling = self._pipeline_result.state is PipelineState.CANCELLING
    return is_terminated or (is_triggered and is_cancelling)

  def is_running(self):
    with self._result_lock:
      return self._pipeline_result.state is PipelineState.RUNNING

  def cancel(self):
    """Cancels this background source recording job.
    """
    with self._result_lock:
      if not PipelineState.is_terminal(self._pipeline_result.state):
        try:
          self._pipeline_result.cancel()
        except NotImplementedError:
          # Ignore the cancel invocation if it is never implemented by the
          # runner.
          pass

  @property
  def state(self):
    with self._result_lock:
      return self._pipeline_result.state


def attempt_to_run_background_caching_job(
    runner, user_pipeline, options=None, limiters=None):
  """Attempts to run a background source recording job for a user-defined
  pipeline.

  Returns True if a job was started, False otherwise.

  The pipeline result is automatically tracked by Interactive Beam in case
  future cancellation/cleanup is needed.
  """
  if is_background_caching_job_needed(user_pipeline):
    # Cancel non-terminal jobs if there is any before starting a new one.
    attempt_to_cancel_background_caching_job(user_pipeline)
    # Cancel the gRPC server serving the test stream if there is one.
    attempt_to_stop_test_stream_service(user_pipeline)
    # TODO(BEAM-8335): refactor background source recording job logic from
    # pipeline_instrument module to this module and aggregate tests.
    from apache_beam.runners.interactive import pipeline_instrument as instr
    runner_pipeline = beam.pipeline.Pipeline.from_runner_api(
        user_pipeline.to_runner_api(), runner, options)
    ie.current_env().add_derived_pipeline(user_pipeline, runner_pipeline)
    background_caching_job_result = beam.pipeline.Pipeline.from_runner_api(
        instr.build_pipeline_instrument(
            runner_pipeline).background_caching_pipeline_proto(),
        runner,
        options).run()

    recording_limiters = (
        limiters
        if limiters else ie.current_env().options.capture_control.limiters())
    ie.current_env().set_background_caching_job(
        user_pipeline,
        BackgroundCachingJob(
            background_caching_job_result, limiters=recording_limiters))
    return True
  return False


def is_background_caching_job_needed(user_pipeline):
  """Determines if a background source recording job needs to be started.

  It does several state checks and recording state changes throughout the
  process. It is not idempotent to simplify the usage.
  """
  job = ie.current_env().get_background_caching_job(user_pipeline)
  # Checks if the pipeline contains any source that needs to be cached.
  need_cache = has_source_to_cache(user_pipeline)
  # If this is True, we can invalidate a previous done/running job if there is
  # one.
  cache_changed = is_source_to_cache_changed(user_pipeline)
  # When recording replay is disabled, cache is always needed for recordable
  # sources (if any).
  if need_cache and not ie.current_env().options.enable_recording_replay:
    from apache_beam.runners.interactive.options import capture_control
    capture_control.evict_captured_data()
    return True
  return (
      need_cache and
      # Checks if it's the first time running a job from the pipeline.
      (
          not job or
          # Or checks if there is no previous job.
          # DONE means a previous job has completed successfully and the
          # cached events might still be valid.
          not (
              job.is_done() or
              # RUNNING means a previous job has been started and is still
              # running.
              job.is_running()) or
          # Or checks if we can invalidate the previous job.
          cache_changed))


def is_cache_complete(pipeline_id):
  # type: (str) -> bool

  """Returns True if the backgrond cache for the given pipeline is done.
  """
  user_pipeline = ie.current_env().pipeline_id_to_pipeline(pipeline_id)
  job = ie.current_env().get_background_caching_job(user_pipeline)
  is_done = job and job.is_done()
  cache_changed = is_source_to_cache_changed(
      user_pipeline, update_cached_source_signature=False)

  # Stop reading from the cache if the background job is done or the underlying
  # cache signature changed that requires a new background source recording job.
  return is_done or cache_changed


def has_source_to_cache(user_pipeline):
  """Determines if a user-defined pipeline contains any source that need to be
  cached. If so, also immediately wrap current cache manager held by current
  interactive environment into a streaming cache if this has not been done.
  The wrapping doesn't invalidate existing cache in any way.

  This can help determining if a background source recording job is needed to
  write cache for sources and if a test stream service is needed to serve the
  cache.

  Throughout the check, if source-to-cache has changed from the last check, it
  also cleans up the invalidated cache early on.
  """
  # TODO(BEAM-8335): we temporarily only cache replaceable unbounded sources.
  # Add logic for other cacheable sources here when they are available.
  has_cache = utils.has_unbounded_sources(user_pipeline)
  if has_cache:
    if not isinstance(ie.current_env().get_cache_manager(user_pipeline,
                                                         create_if_absent=True),
                      streaming_cache.StreamingCache):

      file_based_cm = ie.current_env().get_cache_manager(user_pipeline)
      ie.current_env().set_cache_manager(
          streaming_cache.StreamingCache(
              file_based_cm._cache_dir,
              is_cache_complete=is_cache_complete,
              sample_resolution_sec=1.0,
              saved_pcoders=file_based_cm._saved_pcoders),
          user_pipeline)
  return has_cache


def attempt_to_cancel_background_caching_job(user_pipeline):
  """Attempts to cancel background source recording job for a user-defined
  pipeline.

  If no background source recording job needs to be cancelled, NOOP. Otherwise,
  cancel such job.
  """
  job = ie.current_env().get_background_caching_job(user_pipeline)
  if job:
    job.cancel()


def attempt_to_stop_test_stream_service(user_pipeline):
  """Attempts to stop the gRPC server/service serving the test stream.

  If there is no such server started, NOOP. Otherwise, stop it.
  """
  if is_a_test_stream_service_running(user_pipeline):
    ie.current_env().evict_test_stream_service_controller(user_pipeline).stop()


def is_a_test_stream_service_running(user_pipeline):
  """Checks to see if there is a gPRC server/service running that serves the
  test stream to any job started from the given user_pipeline.
  """
  return ie.current_env().get_test_stream_service_controller(
      user_pipeline) is not None


def is_source_to_cache_changed(
    user_pipeline, update_cached_source_signature=True):
  """Determines if there is any change in the sources that need to be cached
  used by the user-defined pipeline.

  Due to the expensiveness of computations and for the simplicity of usage, this
  function is not idempotent because Interactive Beam automatically discards
  previously tracked signature of transforms and tracks the current signature of
  transforms for the user-defined pipeline if there is any change.

  When it's True, there is addition/deletion/mutation of source transforms that
  requires a new background source recording job.
  """
  # By default gets empty set if the user_pipeline is first time seen because
  # we can treat it as adding transforms.
  recorded_signature = ie.current_env().get_cached_source_signature(
      user_pipeline)
  current_signature = extract_source_to_cache_signature(user_pipeline)
  is_changed = not current_signature.issubset(recorded_signature)
  # The computation of extract_unbounded_source_signature is expensive, track on
  # change by default.
  if is_changed and update_cached_source_signature:
    options = ie.current_env().options
    # No info needed when recording replay is disabled.
    if options.enable_recording_replay:
      if not recorded_signature:

        def sizeof_fmt(num, suffix='B'):
          for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 1000.0:
              return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1000.0
          return "%.1f%s%s" % (num, 'Yi', suffix)

        _LOGGER.info(
            'Interactive Beam has detected unbounded sources in your pipeline. '
            'In order to have a deterministic replay, a segment of data will '
            'be recorded from all sources for %s seconds or until a total of '
            '%s have been written to disk.',
            options.recording_duration.total_seconds(),
            sizeof_fmt(options.recording_size_limit))
      else:
        _LOGGER.info(
            'Interactive Beam has detected a new streaming source was '
            'added to the pipeline. In order for the cached streaming '
            'data to start at the same time, all recorded data has been '
            'cleared and a new segment of data will be recorded.')

    ie.current_env().cleanup(user_pipeline)
    ie.current_env().set_cached_source_signature(
        user_pipeline, current_signature)
    ie.current_env().add_user_pipeline(user_pipeline)
  return is_changed


def extract_source_to_cache_signature(user_pipeline):
  """Extracts a set of signature for sources that need to be cached in the
  user-defined pipeline.

  A signature is a str representation of urn and payload of a source.
  """
  # TODO(BEAM-8335): we temporarily only cache replaceable unbounded sources.
  # Add logic for other cacheable sources here when they are available.
  unbounded_sources_as_applied_transforms = utils.unbounded_sources(
      user_pipeline)
  unbounded_sources_as_ptransforms = set(
      map(lambda x: x.transform, unbounded_sources_as_applied_transforms))
  _, context = user_pipeline.to_runner_api(return_context=True)
  signature = set(
      map(
          lambda transform: str(transform.to_runner_api(context)),
          unbounded_sources_as_ptransforms))
  return signature
