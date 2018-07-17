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

import threading
import time

from apache_beam.runners.interactive import interactive_pipeline_graph

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

  def __init__(self, pipeline_info, pipeline_proto, caches_used, cache_manager,
               referenced_pcollections, required_transforms):
    """Constructor of DisplayManager.

    Args:
      pipeline_info: (interactive_runner.PipelineInfo)
      pipeline_proto: (Pipeline proto)
      caches_used: (set of str) A set of PCollection IDs of those whose cached
          results are used in the execution.
      cache_manager: (interactive_runner.CacheManager) DisplayManager fetches
          the latest status of pipeline execution by querying cache_manager.
      referenced_pcollections: (dict from str to PCollection proto) PCollection
          ID mapped to PCollection referenced during pipeline execution.
      required_transforms: (dict from str to PTransform proto) Mapping from
          transform ID to transforms that leads to visible results.
    """
    # Every parameter except cache_manager is expected to remain constant.
    self._pipeline_info = pipeline_info
    self._pipeline_proto = pipeline_proto
    self._caches_used = caches_used
    self._cache_manager = cache_manager
    self._referenced_pcollections = referenced_pcollections
    self._required_transforms = required_transforms

    self._pcollection_stats = {}

    self._producers = {}
    for _, transform in pipeline_proto.components.transforms.items():
      for pcoll_id in transform.outputs.values():
        self._producers[pcoll_id] = transform.unique_name

    # To be printed.
    self._status = (
        'Using %s cached PCollections\nExecuting %s of %s '
        'transforms.') % (
            len(caches_used), len(required_transforms) - len(caches_used) - 1,
            len([
                t for t in pipeline_proto.components.transforms.values()
                if not t.subtransforms
            ]))
    self._text_samples = []

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
      new_stats = {}
      for pcoll_id in self._pipeline_info.all_pcollections():
        cache_label = self._pipeline_info.derivation(pcoll_id).cache_label()
        if (pcoll_id not in self._pcollection_stats and
            self._cache_manager.exists('sample', cache_label)):
          contents = list(
              self._cache_manager.read('sample', cache_label))
          new_stats[pcoll_id] = {'sample': contents}
          if pcoll_id in self._referenced_pcollections:
            self._text_samples.append(str(
                '%s produced %s' % (
                    self._producers[pcoll_id],
                    interactive_pipeline_graph.format_sample(contents, 5))))
      if force or new_stats:
        if IPython:
          IPython.core.display.clear_output(True)

        self._pcollection_stats.update(new_stats)
        # TODO(qinyeli): Enable updating pipeline graph instead of constructing
        # everytime, if it worths.

        pipeline_graph = interactive_pipeline_graph.InteractivePipelineGraph(
            self._pipeline_proto,
            required_transforms=self._required_transforms,
            referenced_pcollections=self._referenced_pcollections,
            cached_pcollections=self._caches_used,
            pcollection_stats=self._pcollection_stats)
        pipeline_graph.display_graph()

        _display_progress('Running...')
        _display_progress(self._status)
        for text_sample in self._text_samples:
          _display_progress(text_sample)

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
    self.update_display()
    self._periodic_update = False
