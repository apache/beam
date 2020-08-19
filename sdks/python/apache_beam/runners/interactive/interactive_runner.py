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

"""A runner that allows running of Beam pipelines interactively.

This module is experimental. No backwards-compatibility guarantees.
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import apache_beam as beam
from apache_beam import runners
from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as inst
from apache_beam.runners.interactive import background_caching_job
from apache_beam.runners.interactive.display import pipeline_graph
from apache_beam.runners.interactive.options import capture_control
from apache_beam.runners.interactive.utils import to_element_list
from apache_beam.testing.test_stream_service import TestStreamServiceController

# size of PCollection samples cached.
SAMPLE_SIZE = 8

_LOGGER = logging.getLogger(__name__)


class InteractiveRunner(runners.PipelineRunner):
  """An interactive runner for Beam Python pipelines.

  Allows interactively building and running Beam Python pipelines.
  """
  def __init__(
      self,
      underlying_runner=None,
      render_option=None,
      skip_display=True,
      force_compute=True,
      blocking=True):
    """Constructor of InteractiveRunner.

    Args:
      underlying_runner: (runner.PipelineRunner)
      render_option: (str) this parameter decides how the pipeline graph is
          rendered. See display.pipeline_graph_renderer for available options.
      skip_display: (bool) whether to skip display operations when running the
          pipeline. Useful if running large pipelines when display is not
          needed.
      force_compute: (bool) whether sequential pipeline runs can use cached data
          of PCollections computed from the previous runs including show API
          invocation from interactive_beam module. If True, always run the whole
          pipeline and compute data for PCollections forcefully. If False, use
          available data and run minimum pipeline fragment to only compute data
          not available.
      blocking: (bool) whether the pipeline run should be blocking or not.
    """
    self._underlying_runner = (
        underlying_runner or direct_runner.DirectRunner())
    self._render_option = render_option
    self._in_session = False
    self._skip_display = skip_display
    self._force_compute = force_compute
    self._blocking = blocking

  def is_fnapi_compatible(self):
    # TODO(BEAM-8436): return self._underlying_runner.is_fnapi_compatible()
    return False

  def set_render_option(self, render_option):
    """Sets the rendering option.

    Args:
      render_option: (str) this parameter decides how the pipeline graph is
          rendered. See display.pipeline_graph_renderer for available options.
    """
    self._render_option = render_option

  def start_session(self):
    """Start the session that keeps back-end managers and workers alive.
    """
    if self._in_session:
      return

    enter = getattr(self._underlying_runner, '__enter__', None)
    if enter is not None:
      _LOGGER.info('Starting session.')
      self._in_session = True
      enter()
    else:
      _LOGGER.error('Keep alive not supported.')

  def end_session(self):
    """End the session that keeps backend managers and workers alive.
    """
    if not self._in_session:
      return

    exit = getattr(self._underlying_runner, '__exit__', None)
    if exit is not None:
      self._in_session = False
      _LOGGER.info('Ending session.')
      exit(None, None, None)

  def apply(self, transform, pvalueish, options):
    # TODO(qinyeli, BEAM-646): Remove runner interception of apply.
    return self._underlying_runner.apply(transform, pvalueish, options)

  def run_pipeline(self, pipeline, options):
    if not ie.current_env().options.enable_capture_replay:
      capture_control.evict_captured_data()
    if self._force_compute:
      ie.current_env().evict_computed_pcollections()

    # Make sure that sources without a user reference are still cached.
    inst.watch_sources(pipeline)

    user_pipeline = inst.user_pipeline(pipeline)
    pipeline_instrument = inst.build_pipeline_instrument(pipeline, options)

    # The user_pipeline analyzed might be None if the pipeline given has nothing
    # to be cached and tracing back to the user defined pipeline is impossible.
    # When it's None, there is no need to cache including the background
    # caching job and no result to track since no background caching job is
    # started at all.
    if user_pipeline:
      # Should use the underlying runner and run asynchronously.
      background_caching_job.attempt_to_run_background_caching_job(
          self._underlying_runner, user_pipeline, options)
      if (background_caching_job.has_source_to_cache(user_pipeline) and
          not background_caching_job.is_a_test_stream_service_running(
              user_pipeline)):
        streaming_cache_manager = ie.current_env().get_cache_manager(
            user_pipeline)

        # Only make the server if it doesn't exist already.
        if (streaming_cache_manager and
            not ie.current_env().get_test_stream_service_controller(
                user_pipeline)):

          def exception_handler(e):
            _LOGGER.error(str(e))
            return True

          test_stream_service = TestStreamServiceController(
              streaming_cache_manager, exception_handler=exception_handler)
          test_stream_service.start()
          ie.current_env().set_test_stream_service_controller(
              user_pipeline, test_stream_service)

    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        pipeline_instrument.instrumented_pipeline_proto(),
        self._underlying_runner,
        options)

    if ie.current_env().get_test_stream_service_controller(user_pipeline):
      endpoint = ie.current_env().get_test_stream_service_controller(
          user_pipeline).endpoint

      # TODO: make the StreamingCacheManager and TestStreamServiceController
      # constructed when the InteractiveEnvironment is imported.
      class TestStreamVisitor(PipelineVisitor):
        def visit_transform(self, transform_node):
          from apache_beam.testing.test_stream import TestStream
          if (isinstance(transform_node.transform, TestStream) and
              not transform_node.transform._events):
            transform_node.transform._endpoint = endpoint

      pipeline_to_execute.visit(TestStreamVisitor())

    if not self._skip_display:
      a_pipeline_graph = pipeline_graph.PipelineGraph(
          pipeline_instrument.original_pipeline,
          render_option=self._render_option)
      a_pipeline_graph.display_graph()

    main_job_result = PipelineResult(
        pipeline_to_execute.run(), pipeline_instrument)
    # In addition to this pipeline result setting, redundant result setting from
    # outer scopes are also recommended since the user_pipeline might not be
    # available from within this scope.
    if user_pipeline:
      ie.current_env().set_pipeline_result(user_pipeline, main_job_result)

    if self._blocking:
      main_job_result.wait_until_finish()

    if main_job_result.state is beam.runners.runner.PipelineState.DONE:
      # pylint: disable=dict-values-not-iterating
      ie.current_env().mark_pcollection_computed(
          pipeline_instrument.runner_pcoll_to_user_pcoll.values())

    return main_job_result


class PipelineResult(beam.runners.runner.PipelineResult):
  """Provides access to information about a pipeline."""
  def __init__(self, underlying_result, pipeline_instrument):
    """Constructor of PipelineResult.

    Args:
      underlying_result: (PipelineResult) the result returned by the underlying
          runner running the pipeline.
      pipeline_instrument: (PipelineInstrument) pipeline instrument describing
          the pipeline being executed with interactivity applied and related
          metadata including where the interactivity-backing cache lies.
    """
    super(PipelineResult, self).__init__(underlying_result.state)
    self._underlying_result = underlying_result
    self._pipeline_instrument = pipeline_instrument

  @property
  def state(self):
    return self._underlying_result.state

  def wait_until_finish(self):
    self._underlying_result.wait_until_finish()

  def get(self, pcoll, include_window_info=False):
    """Materializes the PCollection into a list.

    If include_window_info is True, then returns the elements as
    WindowedValues. Otherwise, return the element as itself.
    """
    return list(self.read(pcoll, include_window_info))

  def read(self, pcoll, include_window_info=False):
    """Reads the PCollection one element at a time from cache.

    If include_window_info is True, then returns the elements as
    WindowedValues. Otherwise, return the element as itself.
    """
    key = self._pipeline_instrument.cache_key(pcoll)
    cache_manager = ie.current_env().get_cache_manager(
        self._pipeline_instrument.user_pipeline)
    if cache_manager.exists('full', key):
      coder = cache_manager.load_pcoder('full', key)
      reader, _ = cache_manager.read('full', key)
      return to_element_list(reader, coder, include_window_info)
    else:
      raise ValueError('PCollection not available, please run the pipeline.')

  def cancel(self):
    self._underlying_result.cancel()
