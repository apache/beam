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
from apache_beam.runners.interactive import pipeline_analyzer
from apache_beam.runners.interactive.display import display_manager
from apache_beam.runners.interactive.display import pipeline_graph_renderer

# size of PCollection samples cached.
SAMPLE_SIZE = 8


class InteractiveRunner(runners.PipelineRunner):
  """An interactive runner for Beam Python pipelines.

  Allows interactively building and running Beam Python pipelines.
  """

  def __init__(self, underlying_runner=None, cache_dir=None,
               render_option=None):
    """Constructor of InteractiveRunner.

    Args:
      underlying_runner: (runner.PipelineRunner)
      cache_dir: (str) the directory where PCollection caches are kept
      render_option: (str) this parameter decides how the pipeline graph is
          rendered. See display.pipeline_graph_renderer for available options.
    """
    self._underlying_runner = (underlying_runner
                               or direct_runner.DirectRunner())
    self._cache_manager = cache.FileBasedCacheManager(cache_dir)
    self._renderer = pipeline_graph_renderer.get_renderer(render_option)
    self._in_session = False

  def set_render_option(self, render_option):
    """Sets the rendering option.

    Args:
      render_option: (str) this parameter decides how the pipeline graph is
          rendered. See display.pipeline_graph_renderer for available options.
    """
    self._renderer = pipeline_graph_renderer.get_renderer(render_option)

  def start_session(self):
    """Start the session that keeps back-end managers and workers alive.
    """
    if self._in_session:
      return

    enter = getattr(self._underlying_runner, '__enter__', None)
    if enter is not None:
      logging.info('Starting session.')
      self._in_session = True
      enter()
    else:
      logging.error('Keep alive not supported.')

  def end_session(self):
    """End the session that keeps backend managers and workers alive.
    """
    if not self._in_session:
      return

    exit = getattr(self._underlying_runner, '__exit__', None)
    if exit is not None:
      self._in_session = False
      logging.info('Ending session.')
      exit(None, None, None)

  def cleanup(self):
    self._cache_manager.cleanup()

  def apply(self, transform, pvalueish, options):
    # TODO(qinyeli, BEAM-646): Remove runner interception of apply.
    return self._underlying_runner.apply(transform, pvalueish, options)

  def run_pipeline(self, pipeline, options):
    if not hasattr(self, '_desired_cache_labels'):
      self._desired_cache_labels = set()

    # Invoke a round trip through the runner API. This makes sure the Pipeline
    # proto is stable.
    pipeline = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(use_fake_coders=True),
        pipeline.runner,
        options)

    # Snapshot the pipeline in a portable proto before mutating it.
    pipeline_proto, original_context = pipeline.to_runner_api(
        return_context=True, use_fake_coders=True)
    pcolls_to_pcoll_id = self._pcolls_to_pcoll_id(pipeline, original_context)

    analyzer = pipeline_analyzer.PipelineAnalyzer(self._cache_manager,
                                                  pipeline_proto,
                                                  self._underlying_runner,
                                                  options,
                                                  self._desired_cache_labels)
    # Should be only accessed for debugging purpose.
    self._analyzer = analyzer

    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        self._underlying_runner,
        options)

    display = display_manager.DisplayManager(
        pipeline_proto=pipeline_proto,
        pipeline_analyzer=analyzer,
        cache_manager=self._cache_manager,
        pipeline_graph_renderer=self._renderer)
    display.start_periodic_update()
    result = pipeline_to_execute.run()
    result.wait_until_finish()
    display.stop_periodic_update()

    return PipelineResult(result, self, self._analyzer.pipeline_info(),
                          self._cache_manager, pcolls_to_pcoll_id)

  def _pcolls_to_pcoll_id(self, pipeline, original_context):
    """Returns a dict mapping PCollections string to PCollection IDs.

    Using a PipelineVisitor to iterate over every node in the pipeline,
    records the mapping from PCollections to PCollections IDs. This mapping
    will be used to query cached PCollections.

    Args:
      pipeline: (pipeline.Pipeline)
      original_context: (pipeline_context.PipelineContext)

    Returns:
      (dict from str to str) a dict mapping str(pcoll) to pcoll_id.
    """
    pcolls_to_pcoll_id = {}

    from apache_beam.pipeline import PipelineVisitor  # pylint: disable=import-error

    class PCollVisitor(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that records input and output values to be replaced.

      Input and output values that should be updated are recorded in maps
      input_replacements and output_replacements respectively.

      We cannot update input and output values while visiting since that
      results in validation errors.
      """

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        for pcoll in transform_node.outputs.values():
          pcolls_to_pcoll_id[str(pcoll)] = original_context.pcollections.get_id(
              pcoll)

    pipeline.visit(PCollVisitor())
    return pcolls_to_pcoll_id


class PipelineResult(beam.runners.runner.PipelineResult):
  """Provides access to information about a pipeline."""

  def __init__(self, underlying_result, runner, pipeline_info, cache_manager,
               pcolls_to_pcoll_id):
    super(PipelineResult, self).__init__(underlying_result.state)
    self._runner = runner
    self._pipeline_info = pipeline_info
    self._cache_manager = cache_manager
    self._pcolls_to_pcoll_id = pcolls_to_pcoll_id

  def _cache_label(self, pcoll):
    pcoll_id = self._pcolls_to_pcoll_id[str(pcoll)]
    return self._pipeline_info.cache_label(pcoll_id)

  def wait_until_finish(self):
    # PipelineResult is not constructed until pipeline execution is finished.
    return

  def get(self, pcoll):
    cache_label = self._cache_label(pcoll)
    if self._cache_manager.exists('full', cache_label):
      pcoll_list, _ = self._cache_manager.read('full', cache_label)
      return pcoll_list
    else:
      self._runner._desired_cache_labels.add(cache_label)  # pylint: disable=protected-access
      raise ValueError('PCollection not available, please run the pipeline.')

  def sample(self, pcoll):
    cache_label = self._cache_label(pcoll)
    if self._cache_manager.exists('sample', cache_label):
      return self._cache_manager.read('sample', cache_label)
    else:
      self._runner._desired_cache_labels.add(cache_label)  # pylint: disable=protected-access
      raise ValueError('PCollection not available, please run the pipeline.')
