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

# pytype: skip-file

import collections
import glob
import io
import tempfile
from typing import Iterable

from apache_beam import pvalue
from apache_beam.transforms import window
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import WindowInto
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.ptransform import ptransform_fn
from apache_beam.transforms.util import CoGroupByKey

__all__ = [
    'assert_that',
    'equal_to',
    'equal_to_per_window',
    'is_empty',
    'is_not_empty',
    'matches_all',
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

    def __hash__(self):
      return hash(self._counter)

    def __repr__(self):
      return "InAnyOrder(%s)" % self._counter

  return InAnyOrder(iterable)


class _EqualToPerWindowMatcher(object):
  def __init__(self, expected_window_to_elements):
    self._expected_window_to_elements = expected_window_to_elements

  def __call__(self, value):
    # Short-hand.
    _expected = self._expected_window_to_elements

    # Match the given windowed value to an expected window. Fails if the window
    # doesn't exist or the element wasn't found in the window.
    def match(windowed_value):
      actual = windowed_value.value
      window_key = windowed_value.windows[0]
      try:
        _expected[window_key]
      except KeyError:
        raise BeamAssertException(
            'Failed assert: window {} not found in any expected ' \
            'windows {}'.format(window_key, list(_expected.keys())))\

      # Remove any matched elements from the window. This is used later on to
      # assert that all elements in the window were matched with actual
      # elements.
      try:
        _expected[window_key].remove(actual)
      except ValueError:
        raise BeamAssertException(
            'Failed assert: element {} not found in window ' \
            '{}:{}'.format(actual, window_key, _expected[window_key]))\

    # Run the matcher for each window and value pair. Fails if the
    # windowed_value is not a TestWindowedValue.
    for windowed_value in value:
      if not isinstance(windowed_value, TestWindowedValue):
        raise BeamAssertException(
            'Failed assert: Received element {} is not of type ' \
            'TestWindowedValue. Did you forget to set reify_windows=True ' \
            'on the assertion?'.format(windowed_value))
      match(windowed_value)

    # Finally, some elements may not have been matched. Assert that we removed
    # all the elements that we received from the expected list. If the list is
    # non-empty, then there are unmatched elements.
    for win in _expected:
      if _expected[win]:
        raise BeamAssertException(
            'Failed assert: unmatched elements {} in window {}'.format(
                _expected[win], win))


def equal_to_per_window(expected_window_to_elements):
  """Matcher used by assert_that to check to assert expected windows.

  The 'assert_that' statement must have reify_windows=True. This assertion works
  when elements are emitted and are finally checked at the end of the window.

  Arguments:
    expected_window_to_elements: A dictionary where the keys are the windows
      to check and the values are the elements associated with each window.
  """

  return _EqualToPerWindowMatcher(expected_window_to_elements)


# Note that equal_to checks if expected and actual are permutations of each
# other. However, only permutations of the top level are checked. Therefore
# [1,2] and [2,1] are considered equal and [[1,2]] and [[2,1]] are not.
def equal_to(expected, equals_fn=None):
  def _equal(actual, equals_fn=equals_fn):
    expected_list = list(expected)

    # Try to compare actual and expected by sorting. This fails with a
    # TypeError in Python 3 if different types are present in the same
    # collection. It can also raise false negatives for types that don't have
    # a deterministic sort order, like pyarrow Tables as of 0.14.1
    if not equals_fn:
      equals_fn = lambda e, a: e == a
      try:
        sorted_expected = sorted(expected)
        sorted_actual = sorted(actual)
        if sorted_expected == sorted_actual:
          return
      except TypeError:
        pass
    # Slower method, used in two cases:
    # 1) If sorted expected != actual, use this method to verify the inequality.
    #    This ensures we don't raise any false negatives for types that don't
    #    have a deterministic sort order.
    # 2) As a fallback if we encounter a TypeError in python 3. this method
    #    works on collections that have different types.
    unexpected = []
    for element in actual:
      found = False
      for i, v in enumerate(expected_list):
        if equals_fn(v, element):
          found = True
          expected_list.pop(i)
          break
      if not found:
        unexpected.append(element)
    if unexpected or expected_list:
      msg = 'Failed assert: %r == %r' % (expected, actual)
      if unexpected:
        msg = msg + ', unexpected elements %r' % unexpected
      if expected_list:
        msg = msg + ', missing elements %r' % expected_list
      raise BeamAssertException(msg)

  return _equal


def matches_all(expected):
  """Matcher used by assert_that to check a set of matchers.

  Args:
    expected: A list of elements or hamcrest matchers to be used to match
      the elements of a single PCollection.
  """
  def _matches(actual):
    from hamcrest.core import assert_that as hamcrest_assert
    from hamcrest.library.collection import contains_inanyorder
    expected_list = list(expected)

    hamcrest_assert(actual, contains_inanyorder(*expected_list))

  return _matches


def is_empty():
  def _empty(actual):
    actual = list(actual)
    if actual:
      raise BeamAssertException('Failed assert: [] == %r' % actual)

  return _empty


def is_not_empty():
  """
  This is test method which makes sure that the pcol is not empty and it has
  some data in it.
  :return:
  """
  def _not_empty(actual):
    actual = list(actual)
    if not actual:
      raise BeamAssertException('Failed assert: pcol is empty')

  return _not_empty


def assert_that(
    actual,
    matcher,
    label='assert_that',
    reify_windows=False,
    use_global_window=True):
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
  assert isinstance(actual, pvalue.PCollection), (
      '%s is not a supported type for Beam assert' % type(actual))
  pipeline = actual.pipeline
  if getattr(actual.pipeline, 'result', None):
    # The pipeline was already run. The user most likely called assert_that
    # after the pipeleline context.
    raise RuntimeError(
        'assert_that must be used within a beam.Pipeline context')

  # Usually, the uniqueness of the label is left to the pipeline
  # writer to guarantee. Since we're in a testing context, we'll
  # just automatically append a number to the label if it's
  # already in use, as tests don't typically have to worry about
  # long-term update compatibility stability of stage names.
  if label in pipeline.applied_labels:
    label_idx = 2
    while f"{label}_{label_idx}" in pipeline.applied_labels:
      label_idx += 1
    label = f"{label}_{label_idx}"

  if isinstance(matcher, _EqualToPerWindowMatcher):
    reify_windows = True
    use_global_window = True

  class ReifyTimestampWindow(DoFn):
    def process(
        self, element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
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
      keyed_singleton.is_bounded = True

      if use_global_window:
        pcoll = pcoll | WindowInto(window.GlobalWindows())

      keyed_actual = pcoll | 'ToVoidKey' >> Map(lambda v: (None, v))
      keyed_actual.is_bounded = True

      # This is a CoGroupByKey so that the matcher always runs, even if the
      # PCollection is empty.
      plain_actual = ((keyed_singleton, keyed_actual)
                      | 'Group' >> CoGroupByKey()
                      | 'Unkey' >> Map(lambda k_values: k_values[1][1]))

      if not use_global_window:
        plain_actual = plain_actual | 'AddWindow' >> ParDo(AddWindow())

      return plain_actual | 'Match' >> Map(matcher)

    def default_label(self):
      return label

  return actual | AssertThat()


@ptransform_fn
def AssertThat(pcoll, *args, **kwargs):
  """Like assert_that, but as an applicable PTransform."""
  return assert_that(pcoll, *args, **kwargs)


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


def _sort_lists(result):
  if isinstance(result, list):
    return sorted(result)
  elif isinstance(result, tuple):
    return tuple(_sort_lists(e) for e in result)
  elif isinstance(result, dict):
    return {k: _sort_lists(v) for k, v in result.items()}
  elif isinstance(result, Iterable) and not isinstance(result, str):
    return sorted(result)
  else:
    return result


# A utility transform that recursively sorts lists for easier testing.
SortLists = Map(_sort_lists)
