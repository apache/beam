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

"""Internal side input transforms and implementations.

For internal use only; no backwards-compatibility guarantees.

Important: this module is an implementation detail and should not be used
directly by pipeline writers. Instead, users should use the helper methods
AsSingleton, AsIter, AsList and AsDict in apache_beam.pvalue.
"""

# pytype: skip-file

import re
from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import Any

from apache_beam.transforms import window

if TYPE_CHECKING:
  from apache_beam import pvalue

WindowMappingFn = Callable[[window.BoundedWindow], window.BoundedWindow]

SIDE_INPUT_PREFIX = 'python_side_input'

SIDE_INPUT_REGEX = SIDE_INPUT_PREFIX + '([0-9]+)(-.*)?$'


# Top-level function so we can identify it later.
def _global_window_mapping_fn(
    w, global_window=window.GlobalWindow()) -> window.GlobalWindow:
  return global_window


def default_window_mapping_fn(
    target_window_fn: window.WindowFn) -> WindowMappingFn:
  if target_window_fn == window.GlobalWindows():
    return _global_window_mapping_fn

  if isinstance(target_window_fn, window.Sessions):
    raise RuntimeError("Sessions is not allowed in side inputs")

  def map_via_end(source_window: window.BoundedWindow) -> window.BoundedWindow:
    return list(
        target_window_fn.assign(
            window.WindowFn.AssignContext(source_window.max_timestamp())))[-1]

  return map_via_end


def get_sideinput_index(tag: str) -> int:
  match = re.match(SIDE_INPUT_REGEX, tag, re.DOTALL)
  if match:
    return int(match.group(1))
  else:
    raise RuntimeError("Invalid tag %r" % tag)


class SideInputMap(object):
  """Represents a mapping of windows to side input values."""
  def __init__(self, view_class: 'pvalue.AsSideInput', view_options, iterable):
    self._window_mapping_fn = view_options.get(
        'window_mapping_fn', _global_window_mapping_fn)
    self._view_class = view_class
    self._view_options = view_options
    self._iterable = iterable
    self._cache: dict[window.BoundedWindow, Any] = {}

  def __getitem__(self, window: window.BoundedWindow) -> Any:
    if window not in self._cache:
      target_window = self._window_mapping_fn(window)
      self._cache[window] = self._view_class._from_runtime_iterable(
          _FilteringIterable(self._iterable, target_window), self._view_options)
    return self._cache[window]

  def is_globally_windowed(self) -> bool:
    return self._window_mapping_fn == _global_window_mapping_fn


class _FilteringIterable(object):
  """An iterable containing only those values in the given window.
  """
  def __init__(self, iterable, target_window):
    self._iterable = iterable
    self._target_window = target_window

  def __iter__(self):
    for wv in self._iterable:
      if self._target_window in wv.windows:
        yield wv.value

  def __reduce__(self):
    # Pickle self as an already filtered list.
    return list, (list(self), )
