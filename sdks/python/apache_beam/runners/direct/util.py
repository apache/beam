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

"""Utility classes used by the DirectRunner.

For internal use only. No backwards compatibility guarantees.
"""

from __future__ import absolute_import


class TransformResult(object):
  """Result of evaluating an AppliedPTransform with a TransformEvaluator."""

  def __init__(self, applied_ptransform, uncommitted_output_bundles,
               unprocessed_bundles, counters, keyed_watermark_holds,
               undeclared_tag_values=None):
    self.transform = applied_ptransform
    self.uncommitted_output_bundles = uncommitted_output_bundles
    self.unprocessed_bundles = unprocessed_bundles
    self.counters = counters
    # Mapping of key -> earliest hold timestamp or None.  Keys should be
    # strings or None.
    #
    # For each key, we receive as its corresponding value the earliest
    # watermark hold for that key (the key can be None for global state), past
    # which the output watermark for the currently-executing step will not
    # advance.  If the value is None or utils.timestamp.MAX_TIMESTAMP, the
    # watermark hold will be removed.
    self.keyed_watermark_holds = keyed_watermark_holds or {}
    # Only used when caching (materializing) all values is requested.
    self.undeclared_tag_values = undeclared_tag_values
    # Populated by the TransformExecutor.
    self.logical_metric_updates = None


class TimerFiring(object):
  """A single instance of a fired timer."""

  def __init__(self, encoded_key, window, name, time_domain, timestamp):
    self.encoded_key = encoded_key
    self.window = window
    self.name = name
    self.time_domain = time_domain
    self.timestamp = timestamp


class KeyedWorkItem(object):
  """A keyed item that can either be a timer firing or a list of elements."""
  def __init__(self, encoded_key, timer_firings=None, elements=None):
    self.encoded_key = encoded_key
    assert not (timer_firings and elements)
    self.timer_firings = timer_firings or []
    self.elements = elements or []
