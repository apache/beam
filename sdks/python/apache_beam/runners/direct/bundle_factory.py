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

"""A factory that creates UncommittedBundles."""

# pytype: skip-file
# mypy: disallow-untyped-defs

from __future__ import absolute_import

from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from typing import cast

from apache_beam import pvalue
from apache_beam.runners import common
from apache_beam.utils.windowed_value import WindowedValue

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow
  from apache_beam.utils.timestamp import Timestamp
  from apache_beam.utils.windowed_value import PaneInfo


class BundleFactory(object):
  """For internal use only; no backwards-compatibility guarantees.

  BundleFactory creates output bundles to be used by transform evaluators.

  Args:
    stacked: whether or not to stack the WindowedValues within the bundle
      in case consecutive ones share the same timestamp and windows.
      DirectRunnerOptions.direct_runner_use_stacked_bundle controls this option.
  """
  def __init__(self, stacked):
    # type: (bool) -> None
    self._stacked = stacked

  def create_bundle(self, output_pcollection):
    # type: (Union[pvalue.PBegin, pvalue.PCollection]) -> _Bundle
    return _Bundle(output_pcollection, self._stacked)

  def create_empty_committed_bundle(self, output_pcollection):
    # type: (Union[pvalue.PBegin, pvalue.PCollection]) -> _Bundle
    bundle = self.create_bundle(output_pcollection)
    bundle.commit(None)
    return bundle


# a bundle represents a unit of work that will be processed by a transform.
class _Bundle(common.Receiver):
  """Part of a PCollection with output elements.

  Part of a PCollection. Elements are output to a bundle, which will cause them
  to be executed by PTransform that consume the PCollection this bundle is a
  part of at a later point. It starts as an uncommitted bundle and can have
  elements added to it. It needs to be committed to make it immutable before
  passing it to a downstream ptransform.

  The stored elements are WindowedValues, which contains timestamp and windows
  information.

  Bundle internally optimizes storage by stacking elements with the same
  timestamp and windows into StackedWindowedValues, and then returns an iterable
  to restore WindowedValues upon get_elements() call.

  When this optimization is not desired, it can be avoided by an option when
  creating bundles, like:::

    b = Bundle(stacked=False)
  """
  class _StackedWindowedValues(object):
    """A stack of WindowedValues with the same timestamp and windows.

    It must be initialized from a single WindowedValue.

    Example:::

      s = StackedWindowedValues(windowed_value)
      if (another_windowed_value.timestamp == s.timestamp and
          another_windowed_value.windows == s.windows):
        s.add_value(another_windowed_value.value)
      windowed_values = [wv for wv in s.windowed_values()]
      # now windowed_values equals to [windowed_value, another_windowed_value]
    """
    def __init__(self, initial_windowed_value):
      # type: (WindowedValue) -> None
      self._initial_windowed_value = initial_windowed_value
      self._appended_values = []  # type: List[Any]

    @property
    def timestamp(self):
      # type: () -> Timestamp
      return self._initial_windowed_value.timestamp

    @property
    def windows(self):
      # type: () -> Tuple[BoundedWindow, ...]
      return self._initial_windowed_value.windows

    @property
    def pane_info(self):
      # type: () -> PaneInfo
      return self._initial_windowed_value.pane_info

    def add_value(self, value):
      # type: (Any) -> None
      self._appended_values.append(value)

    def windowed_values(self):
      # type: () -> Iterator[WindowedValue]
      # yield first windowed_value as is, then iterate through
      # _appended_values to yield WindowedValue on the fly.
      yield self._initial_windowed_value
      for v in self._appended_values:
        yield self._initial_windowed_value.with_value(v)

  def __init__(self, pcollection, stacked=True):
    # type: (Union[pvalue.PBegin, pvalue.PCollection], bool) -> None
    assert isinstance(pcollection, (pvalue.PBegin, pvalue.PCollection))
    self._pcollection = pcollection
    self._elements = [
    ]  # type: List[Union[WindowedValue, _Bundle._StackedWindowedValues]]
    self._stacked = stacked
    self._committed = False
    self._tag = None  # type: Optional[str]  # optional tag information for this bundle

  def get_elements_iterable(self, make_copy=False):
    # type: (bool) -> Iterable[WindowedValue]

    """Returns iterable elements.

    Args:
      make_copy: whether to force returning copy or yielded iterable.

    Returns:
      unstacked elements,
      in the form of iterable if committed and make_copy is not True,
      or as a list of copied WindowedValues.
    """
    if not self._stacked:
      # we can safely assume self._elements contains only WindowedValues
      elements = cast('List[WindowedValue]', self._elements)
      if self._committed and not make_copy:
        return elements
      return list(elements)

    def iterable_stacked_or_elements(elements):
      # type: (Iterable[Union[WindowedValue, _Bundle._StackedWindowedValues]]) -> Iterator[WindowedValue]
      for e in elements:
        if isinstance(e, _Bundle._StackedWindowedValues):
          for w in e.windowed_values():
            yield w
        else:
          yield e

    if self._committed and not make_copy:
      return iterable_stacked_or_elements(self._elements)
    # returns a copy.
    return [e for e in iterable_stacked_or_elements(self._elements)]

  def has_elements(self):
    # type: () -> int
    return len(self._elements) > 0

  @property
  def tag(self):
    # type: () -> Optional[str]
    return self._tag

  @tag.setter
  def tag(self, value):
    # type: (str) -> None
    assert not self._tag
    self._tag = value

  @property
  def pcollection(self):
    # type: () -> Union[pvalue.PBegin, pvalue.PCollection]

    """PCollection that the elements of this UncommittedBundle belong to."""
    return self._pcollection

  def add(self, element):
    # type: (WindowedValue) -> None

    """Outputs an element to this bundle.

    Args:
      element: WindowedValue
    """
    assert not self._committed
    if not self._stacked:
      self._elements.append(element)
      return
    if (self._elements and
        (isinstance(self._elements[-1],
                    (WindowedValue, _Bundle._StackedWindowedValues))) and
        self._elements[-1].timestamp == element.timestamp and
        self._elements[-1].windows == element.windows and
        self._elements[-1].pane_info == element.pane_info):
      if isinstance(self._elements[-1], WindowedValue):
        self._elements[-1] = _Bundle._StackedWindowedValues(self._elements[-1])
      self._elements[-1].add_value(element.value)
    else:
      self._elements.append(element)

  def output(self, element):
    # type: (WindowedValue) -> None
    self.add(element)

  def receive(self, element):
    # type: (WindowedValue) -> None
    self.add(element)

  def commit(self, synchronized_processing_time):
    # type: (Optional[float]) -> None

    """Commits this bundle.

    Uncommitted bundle will become committed (immutable) after this call.

    Args:
      synchronized_processing_time: the synchronized processing time at which
      this bundle was committed
    """
    assert not self._committed
    self._committed = True
    # typing: overriding list with an immutable tuple is fine here.
    # add() already asserts that _committed is false, but this adds extra
    # protection.
    self._elements = tuple(self._elements)  # type: ignore[assignment]
    self._synchronized_processing_time = synchronized_processing_time
