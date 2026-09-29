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

"""Process-wide switches that stop kinds of user metrics from being reported.

This module is imported by ``apache_beam.metrics.execution`` and by the
pipeline and worker start-up code, so it deliberately keeps its imports to a
minimum to avoid circular imports.
"""

# pytype: skip-file

import logging
from typing import TYPE_CHECKING
from typing import Any
from typing import Set

from apache_beam.metrics import cells

if TYPE_CHECKING:
  from apache_beam.options.pipeline_options import PipelineOptions

__all__ = ['MetricsFlag']

_LOGGER = logging.getLogger(__name__)

# Metric cell types whose updates are dropped process-wide. Empty (the default)
# means every update is delivered. apache_beam.metrics.execution holds a
# reference to this set, so it is only ever mutated in place, never rebound.
DISABLED_CELL_TYPES = set()  # type: Set[Any]


class MetricsFlag(object):
  """Process-wide switches that stop kinds of user metrics from being reported.

  High throughput jobs may want to turn off metrics that put pressure on the
  metrics backend. Mirroring the Java SDK, the ``disableCounterMetrics``,
  ``disableStringSetMetrics`` and ``disableBoundedTrieMetrics`` experiments make
  the corresponding ``Metrics.counter``, ``Metrics.string_set`` and
  ``Metrics.bounded_trie`` updates no-ops. The metric objects themselves are
  unchanged, so code that holds on to them keeps working.
  """
  _EXPERIMENTS = (
      ('disableCounterMetrics', cells.CounterCell, 'Counter'),
      ('disableStringSetMetrics', cells.StringSetCell, 'StringSet'),
      ('disableBoundedTrieMetrics', cells.BoundedTrieCell, 'BoundedTrie'),
  )
  _initialized = False

  @classmethod
  def set_default_pipeline_options(cls, options: 'PipelineOptions') -> None:
    """Initializes the flags from ``options`` if not already done so.

    Called when a ``Pipeline`` is constructed and at SDK worker harness
    start-up. As in the Java SDK, the first call wins so that user code running
    on a worker cannot change the flags the harness was started with.
    """
    if cls._initialized:
      return
    # Imported here rather than at module level, as a precaution against
    # circular imports between apache_beam.metrics and apache_beam.options.
    from apache_beam.options.pipeline_options import DebugOptions
    debug_options = options.view_as(DebugOptions)
    disabled = set()
    for experiment, cell_type, kind in cls._EXPERIMENTS:
      if debug_options.lookup_experiment(experiment):
        disabled.add(cell_type)
        _LOGGER.info('%s metrics are disabled.', kind)
    DISABLED_CELL_TYPES.clear()
    DISABLED_CELL_TYPES.update(disabled)
    cls._initialized = True

  @classmethod
  def counter_disabled(cls) -> bool:
    return cells.CounterCell in DISABLED_CELL_TYPES

  @classmethod
  def string_set_disabled(cls) -> bool:
    return cells.StringSetCell in DISABLED_CELL_TYPES

  @classmethod
  def bounded_trie_disabled(cls) -> bool:
    return cells.BoundedTrieCell in DISABLED_CELL_TYPES

  @classmethod
  def reset(cls) -> None:
    """Clears the flags so the next ``set_default_pipeline_options`` applies."""
    DISABLED_CELL_TYPES.clear()
    cls._initialized = False
