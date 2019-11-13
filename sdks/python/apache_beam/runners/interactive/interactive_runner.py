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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import apache_beam as beam
from apache_beam import runners
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as inst
from apache_beam.runners.interactive.display import pipeline_graph

# size of PCollection samples cached.
SAMPLE_SIZE = 8


_LOGGER = logging.getLogger(__name__)


class InteractiveRunner(runners.PipelineRunner):
  """An interactive runner for Beam Python pipelines.

  Allows interactively building and running Beam Python pipelines.
  """

  def __init__(self,
               underlying_runner=None,
               cache_dir=None,
               cache_format='text',
               render_option=None,
               skip_display=False):
    """Constructor of InteractiveRunner.

    Args:
      underlying_runner: (runner.PipelineRunner)
      cache_dir: (str) the directory where PCollection caches are kept
      cache_format: (str) the file format that should be used for saving
          PCollection caches. Available options are 'text' and 'tfrecord'.
      render_option: (str) this parameter decides how the pipeline graph is
          rendered. See display.pipeline_graph_renderer for available options.
      skip_display: (bool) whether to skip display operations when running the
          pipeline. Useful if running large pipelines when display is not
          needed.
    """
    self._underlying_runner = (underlying_runner
                               or direct_runner.DirectRunner())
    if not ie.current_env().cache_manager():
      ie.current_env().set_cache_manager(
          cache.FileBasedCacheManager(cache_dir,
                                      cache_format))
    self._cache_manager = ie.current_env().cache_manager()
    self._render_option = render_option
    self._in_session = False
    self._skip_display = skip_display

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

  def cleanup(self):
    self._cache_manager.cleanup()

  # The notebook/ipython related logic uses apply() interception as a hook
  # modifying only labels for PTransforms triggering this callback. Please
  # don't remove.
  def apply(self, transform, pvalueish, options):
    # Track user defined pipeline instances in watched scopes so that later when
    # tracking applied PTransforms, we only track for those pipeline instances,
    # excluding any pipeline instances the Beam SDK creates implicitly.
    ie.current_env().track_user_pipelines()

    prompt = None
    if ie.current_env().is_interactive_ready and ie.current_env().is_in_ipython:
      from IPython import get_ipython
      prompt = get_ipython().execution_count
    pipeline = _extract_pipeline_of_pvalueish(pvalueish)
    if not pipeline:
      _LOGGER.warning('Non-fatal error. Failed to track transforms with '
                      'prompt. Cannot figure out the pipeline that the given '
                      'pvalueish %s belongs to.' % pvalueish)
    is_tracking = (prompt
                   and pipeline
                   # We only track for user pipelines at pipeline construction
                   # time, not pipeline execution time.
                   and pipeline in ie.current_env().tracked_user_pipelines)

    # Pre-runner-application, only tracks transform application for user
    # pipelines, not runner pipelines.
    if is_tracking:
      # Clean up the label from applied labels of the pipeline if it's there
      # because no applied transform will be associated to that label anymore.
      if transform.label in pipeline.applied_labels:
        pipeline.applied_labels.remove(transform.label)
      label_with_metadata = 'Cell {}: {}'.format(prompt, transform.label)
      # Below is unlikely to happen unless the user uses duplicated labels
      # within a notebook-cell/ipython-prompt, we treat all PTransforms within
      # a single cell/prompt as different ones and tag them if needed.
      label_with_metadata = ie.current_env().tagged_name(label_with_metadata)
      # Note at this moment, the transform intercepted has already been half-way
      # applied to the stack of AppliedPTransform of the pipeline. We have to
      # change the full_label for it.
      pipeline.transforms_stack[-1].full_label = label_with_metadata
      transform.label = label_with_metadata
      pipeline.applied_labels.add(transform.label)

    # Runner-application.
    applied = self._underlying_runner.apply(transform, pvalueish, options)

    # After-runner-application.
    if is_tracking:
      # Tracking all AppliedPTransforms after the runner-application is done.
      ie.current_env().track_transforms_with_prompt(prompt, pipeline)

    return applied

  def run_pipeline(self, pipeline, options):
    pipeline_instrument = inst.pin(pipeline, options)
    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        pipeline_instrument.instrumented_pipeline_proto(),
        self._underlying_runner,
        options)

    if not self._skip_display:
      a_pipeline_graph = pipeline_graph.PipelineGraph(
          pipeline_instrument.original_pipeline,
          render_option=self._render_option)
      a_pipeline_graph.display_graph()

    result = pipeline_to_execute.run()
    result.wait_until_finish()

    return PipelineResult(result, pipeline_instrument)


def _extract_pipeline_of_pvalueish(pvalueish):
  """Extracts the pipeline that the given pvalueish belongs to."""
  if isinstance(pvalueish, tuple):
    pvalue = pvalueish[0]
  elif isinstance(pvalueish, dict):
    pvalue = next(iter(pvalueish.values()))
  else:
    pvalue = pvalueish
  if hasattr(pvalue, 'pipeline'):
    return pvalue.pipeline
  return None


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

  def wait_until_finish(self):
    self._underlying_result.wait_until_finish()

  def get(self, pcoll):
    key = self._pipeline_instrument.cache_key(pcoll)
    if ie.current_env().cache_manager().exists('full', key):
      pcoll_list, _ = ie.current_env().cache_manager().read('full', key)
      return pcoll_list
    else:
      raise ValueError('PCollection not available, please run the pipeline.')

  def cancel(self):
    self._underlying_result.cancel()
