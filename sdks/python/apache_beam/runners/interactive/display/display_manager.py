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

"""Manages displaying pipeline graph and execution status on the frontend.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import threading
import time

from apache_beam.runners.interactive.display import interactive_pipeline_graph

try:
  import IPython  # pylint: disable=import-error
  # _display_progress defines how outputs are printed on the frontend.
  _display_progress = IPython.display.display

  def _formatter(string, pp, cycle):  # pylint: disable=unused-argument
    pp.text(string)
  plain = get_ipython().display_formatter.formatters['text/plain']  # pylint: disable=undefined-variable
  plain.for_type(str, _formatter)

# NameError is added here because get_ipython() throws "not defined" NameError
# if not started with IPython.
except (ImportError, NameError):
  IPython = None
  _display_progress = print


class DisplayManager(object):
  """Manages displaying pipeline graph and execution status on the frontend."""

  def __init__(self, pipeline_proto, pipeline_analyzer, cache_manager,
               pipeline_graph_renderer):
    """Constructor of DisplayManager.

    Args:
      pipeline_proto: (Pipeline proto)
      pipeline_analyzer: (PipelineAnalyzer) the pipeline analyzer that
          corresponds to this round of execution. This will provide more
          detailed informations about the pipeline
      cache_manager: (interactive_runner.CacheManager) DisplayManager fetches
          the latest status of pipeline execution by querying cache_manager.
      pipeline_graph_renderer: (pipeline_graph_renderer.PipelineGraphRenderer)
          decides how a pipeline graph is rendered.
    """
    # Every parameter except cache_manager is expected to remain constant.
    self._analyzer = pipeline_analyzer
    self._cache_manager = cache_manager
    self._pipeline_graph = interactive_pipeline_graph.InteractivePipelineGraph(
        pipeline_proto,
        required_transforms=self._analyzer.tl_required_trans_ids(),
        referenced_pcollections=self._analyzer.tl_referenced_pcoll_ids(),
        cached_pcollections=self._analyzer.caches_used())
    self._renderer = pipeline_graph_renderer

    # _text_to_print keeps track of information to be displayed.
    self._text_to_print = collections.OrderedDict()
    self._text_to_print['summary'] = (
        'Using %s cached PCollections\nExecuting %s of %s '
        'transforms.') % (
            len(self._analyzer.caches_used()),
            (len(self._analyzer.tl_required_trans_ids())
             - len(self._analyzer.read_cache_ids())
             - len(self._analyzer.write_cache_ids())),
            len(pipeline_proto.components.transforms[
                pipeline_proto.root_transform_ids[0]].subtransforms))
    self._text_to_print.update({
        pcoll_id: "" for pcoll_id
        in self._analyzer.tl_referenced_pcoll_ids()})

    # _pcollection_stats maps pcoll_id to
    # { 'cache_label': cache_label, version': version, 'sample': pcoll_in_list }
    self._pcollection_stats = {}
    for pcoll_id in self._analyzer.tl_referenced_pcoll_ids():
      self._pcollection_stats[pcoll_id] = {
          'cache_label': self._analyzer.pipeline_info().cache_label(pcoll_id),
          'version': -1,
          'sample': []
      }

    self._producers = {}
    for _, transform in pipeline_proto.components.transforms.items():
      for pcoll_id in transform.outputs.values():
        if pcoll_id not in self._producers or '/' not in transform.unique_name:
          self._producers[pcoll_id] = transform.unique_name

    # For periodic update.
    self._lock = threading.Lock()
    self._periodic_update = False

  def update_display(self, force=False):
    """Updates display on the frontend.

    Retrieves the latest execution status by querying CacheManager and updates
    display on the fronend. The assumption is that there is only one pipeline in
    a cell, because it clears up everything in the cell output every update
    cycle.

    Args:
      force: (bool) whether to force updating when no stats change happens.
    """
    with self._lock:
      stats_updated = False

      for pcoll_id, stats in self._pcollection_stats.items():
        cache_label = stats['cache_label']
        version = stats['version']

        if force or not self._cache_manager.is_latest_version(
            version, 'sample', cache_label):
          pcoll_list, version = self._cache_manager.read('sample', cache_label)
          stats['sample'] = pcoll_list
          stats['version'] = version
          stats_updated = True

          if pcoll_id in self._analyzer.tl_referenced_pcoll_ids():
            self._text_to_print[pcoll_id] = (str(
                '%s produced %s' % (
                    self._producers[pcoll_id],
                    interactive_pipeline_graph.format_sample(pcoll_list, 5))))

      if force or stats_updated:
        self._pipeline_graph.update_pcollection_stats(self._pcollection_stats)

        if IPython:
          from IPython.core import display
          display.clear_output(True)
          rendered_graph = self._renderer.render_pipeline_graph(
              self._pipeline_graph)
          display.display(display.HTML(rendered_graph))

        _display_progress('Running...')
        for text in self._text_to_print.values():
          if text != "":
            _display_progress(text)

  def start_periodic_update(self):
    """Start a thread that periodically updates the display."""
    self.update_display(True)
    self._periodic_update = True

    def _updater():
      while self._periodic_update:
        self.update_display()
        time.sleep(.02)

    t = threading.Thread(target=_updater)
    t.daemon = True
    t.start()

  def stop_periodic_update(self):
    """Stop periodically updating the display."""
    self.update_display(True)
    self._periodic_update = False
