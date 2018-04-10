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
import tempfile

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

    def __repr__(self):
      return "InAnyOrder(%s)" % self._counter

  return InAnyOrder(iterable)


# Note that equal_to always sorts the expected and actual since what we
# compare are PCollections for which there is no guaranteed order.
# However the sorting does not go beyond top level therefore [1,2] and [2,1]
# are considered equal and [[1,2]] and [[2,1]] are not.
def equal_to(expected):
  expected = list(expected)

  def _equal(actual):
    sorted_expected = sorted(expected)
    sorted_actual = sorted(actual)
    if sorted_expected != sorted_actual:
      raise BeamAssertException(
          'Failed assert: %r == %r' % (sorted_expected, sorted_actual))
  return _equal


def is_empty():
  def _empty(actual):
    actual = list(actual)
    if actual:
      raise BeamAssertException(
          'Failed assert: [] == %r' % actual)
  return _empty


def assert_that(actual, matcher, label='assert_that', reify_windows=False):
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

  class AssertThat(PTransform):

    def expand(self, pcoll):
      if reify_windows:
        pcoll = pcoll | ParDo(ReifyTimestampWindow())

      # We must have at least a single element to ensure the matcher
      # code gets run even if the input pcollection is empty.
      keyed_singleton = pcoll.pipeline | Create([(None, None)])
      keyed_actual = (
          pcoll
          | WindowInto(window.GlobalWindows())
          | "ToVoidKey" >> Map(lambda v: (None, v)))
      _ = ((keyed_singleton, keyed_actual)
           | "Group" >> CoGroupByKey()
           | "Unkey" >> Map(lambda k___actual_values: k___actual_values[1][1])
           | "Match" >> Map(matcher))

    def default_label(self):
      return label

  actual | AssertThat()  # pylint: disable=expression-not-assigned


@experimental()
def open_shards(glob_pattern):
  """Returns a composite file of all shards matching the given glob pattern."""
  with tempfile.NamedTemporaryFile(delete=False) as out_file:
    for shard in glob.glob(glob_pattern):
      with open(shard) as in_file:
        out_file.write(in_file.read())
    concatenated_file_name = out_file.name
  return open(concatenated_file_name, 'rb')
