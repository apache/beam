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

"""Utilities for testing Beam pipelines."""

from __future__ import absolute_import

import collections
import glob
import io
import tempfile
from builtins import object

from apache_beam import pvalue
from apache_beam.transforms import window
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import WindowInto
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.util import CoGroupByKey
from apache_beam.utils.annotations import experimental

__all__ = [
    'assert_that',
    'equal_to',
    'is_empty',
    # open_shards is internal and has no backwards compatibility guarantees.
    'open_shards',
    'TestWindowedValue',
    ]


class BeamAssertException(Exception):
  """Exception raised by matcher classes used by assert_that transform."""

  pass


# Used for reifying timestamps and windows for assert_that matchers.
TestWindowedValue = collections.namedtuple(
    'TestWindowedValue', 'value timestamp windows')


def contains_in_any_order(iterable):
  """Creates an object that matches another iterable if they both have the
  same count of items.

  Arguments:
    iterable: An iterable of hashable objects.
  """
  class InAnyOrder(object):
    def __init__(self, iterable):
      self._counter = collections.Counter(iterable)

    def __eq__(self, other):
      return self._counter == collections.Counter(other)

    def __ne__(self, other):
      # TODO(BEAM-5949): Needed for Python 2 compatibility.
      return not self == other

    def __hash__(self):
      return hash(self._counter)

    def __repr__(self):
      return "InAnyOrder(%s)" % self._counter

  return InAnyOrder(iterable)


def equal_to_per_window(expected_window_to_elements):
  """Matcher used by assert_that to check on values for specific windows.

  Arguments:
    expected_window_to_elements: A dictionary where the keys are the windows
      to check and the values are the elements associated with each window.
  """
  def matcher(elements):
    actual_elements_in_window, window = elements
    if window in expected_window_to_elements:
      expected_elements_in_window = list(
          expected_window_to_elements[window])
      sorted_expected = sorted(expected_elements_in_window)
      sorted_actual = sorted(actual_elements_in_window)
      if sorted_expected != sorted_actual:
        # Results for the same window don't necessarily come all
        # at once. Hence the same actual window may contain only
        # subsets of the expected elements for the window.
        # For example, in the presence of early triggers.
        if all(elem in sorted_expected for elem in sorted_actual) is False:
          raise BeamAssertException(
              'Failed assert: %r not in %r' % (sorted_actual, sorted_expected))
  return matcher


# Note that equal_to checks if expected and actual are permutations of each
# other. However, only permutations of the top level are checked. Therefore
# [1,2] and [2,1] are considered equal and [[1,2]] and [[2,1]] are not.
def equal_to(expected):

  def _equal(actual):
    expected_list = list(expected)

    # Try to compare actual and expected by sorting. This fails with a
    # TypeError in Python 3 if different types are present in the same
    # collection.
    try:
      sorted_expected = sorted(expected)
      sorted_actual = sorted(actual)
      if sorted_expected != sorted_actual:
        raise BeamAssertException(
            'Failed assert: %r == %r' % (sorted_expected, sorted_actual))
    # Fall back to slower method which works for different types on Python 3.
    except TypeError:
      for element in actual:
        try:
          expected_list.remove(element)
        except ValueError:
          raise BeamAssertException(
              'Failed assert: %r == %r' % (expected, actual))
      if expected_list:
        raise BeamAssertException(
            'Failed assert: %r == %r' % (expected, actual))

  return _equal


def is_empty():
  def _empty(actual):
    actual = list(actual)
    if actual:
      raise BeamAssertException(
          'Failed assert: [] == %r' % actual)
  return _empty


def assert_that(actual, matcher, label='assert_that',
                reify_windows=False, use_global_window=True):
  """A PTransform that checks a PCollection has an expected value.

  Note that assert_that should be used only for testing pipelines since the
  check relies on materializing the entire PCollection being checked.

  Args:
    actual: A PCollection.
    matcher: A matcher function taking as argument the actual value of a
      materialized PCollection. The matcher validates this actual value against
      expectations and raises BeamAssertException if they are not met.
    label: Optional string label. This is needed in case several assert_that
      transforms are introduced in the same pipeline.
    reify_windows: If True, matcher is passed a list of TestWindowedValue.
    use_global_window: If False, matcher is passed a dictionary of
      (k, v) = (window, elements in the window).

  Returns:
    Ignored.
  """
  assert isinstance(actual, pvalue.PCollection)

  class ReifyTimestampWindow(DoFn):
    def process(self, element, timestamp=DoFn.TimestampParam,
                window=DoFn.WindowParam):
      # This returns TestWindowedValue instead of
      # beam.utils.windowed_value.WindowedValue because ParDo will extract
      # the timestamp and window out of the latter.
      return [TestWindowedValue(element, timestamp, [window])]

  class AddWindow(DoFn):
    def process(self, element, window=DoFn.WindowParam):
      yield element, window

  class AssertThat(PTransform):

    def expand(self, pcoll):
      if reify_windows:
        pcoll = pcoll | ParDo(ReifyTimestampWindow())

      keyed_singleton = pcoll.pipeline | Create([(None, None)])

      if use_global_window:
        pcoll = pcoll | WindowInto(window.GlobalWindows())

      keyed_actual = pcoll | "ToVoidKey" >> Map(lambda v: (None, v))

      plain_actual = ((keyed_singleton, keyed_actual)
                      | "Group" >> CoGroupByKey()
                      | "Unkey" >> Map(lambda k_values: k_values[1][1]))

      if not use_global_window:
        plain_actual = plain_actual | "AddWindow" >> ParDo(AddWindow())

      plain_actual = plain_actual | "Match" >> Map(matcher)

    def default_label(self):
      return label

  actual | AssertThat()  # pylint: disable=expression-not-assigned


@experimental()
def open_shards(glob_pattern, mode='rt', encoding='utf-8'):
  """Returns a composite file of all shards matching the given glob pattern.

  Args:
    glob_pattern (str): Pattern used to match files which should be opened.
    mode (str): Specify the mode in which the file should be opened. For
                available modes, check io.open() documentation.
    encoding (str): Name of the encoding used to decode or encode the file.
                    This should only be used in text mode.

  Returns:
    A stream with the contents of the opened files.
  """
  if 'b' in mode:
    encoding = None

  with tempfile.NamedTemporaryFile(delete=False) as out_file:
    for shard in glob.glob(glob_pattern):
      with open(shard, 'rb') as in_file:
        out_file.write(in_file.read())
    concatenated_file_name = out_file.name
  return io.open(concatenated_file_name, mode, encoding=encoding)
