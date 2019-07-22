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

"""A library of basic combiner PTransform subclasses."""

from __future__ import absolute_import
from __future__ import division

import heapq
import itertools
import math
import operator
import random
import sys
import warnings
from builtins import object
from builtins import zip
from functools import cmp_to_key
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

from past.builtins import long

from apache_beam import typehints
from apache_beam.transforms import core
from apache_beam.transforms import cy_combiners
from apache_beam.transforms import ptransform
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'ApproximateQuantiles',
    'Count',
    'Mean',
    'Sample',
    'Top',
    'ToDict',
    'ToList',
    'Latest'
    ]

# Type variables
T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
TimestampType = Union[int, long, float, Timestamp, Duration]


class Mean(object):
  """Combiners for computing arithmetic means of elements."""

  class Globally(ptransform.PTransform):
    """combiners.Mean.Globally computes the arithmetic mean of the elements."""

    def expand(self, pcoll):
      return pcoll | core.CombineGlobally(MeanCombineFn())

  class PerKey(ptransform.PTransform):
    """combiners.Mean.PerKey finds the means of the values for each key."""

    def expand(self, pcoll):
      return pcoll | core.CombinePerKey(MeanCombineFn())


# TODO(laolu): This type signature is overly restrictive. This should be
# more general.
@with_input_types(Union[float, int, long])
@with_output_types(float)
class MeanCombineFn(core.CombineFn):
  """CombineFn for computing an arithmetic mean."""

  def create_accumulator(self):
    return (0, 0)

  def add_input(self, sum_count, element):
    (sum_, count) = sum_count
    return sum_ + element, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum_, count) = sum_count
    if count == 0:
      return float('NaN')
    return sum_ / float(count)

  def for_input_type(self, input_type):
    if input_type is int:
      return cy_combiners.MeanInt64Fn()
    elif input_type is float:
      return cy_combiners.MeanFloatFn()
    return self


class Count(object):
  """Combiners for counting elements."""

  class Globally(ptransform.PTransform):
    """combiners.Count.Globally counts the total number of elements."""

    def expand(self, pcoll):
      return pcoll | core.CombineGlobally(CountCombineFn())

  class PerKey(ptransform.PTransform):
    """combiners.Count.PerKey counts how many elements each unique key has."""

    def expand(self, pcoll):
      return pcoll | core.CombinePerKey(CountCombineFn())

  class PerElement(ptransform.PTransform):
    """combiners.Count.PerElement counts how many times each element occurs."""

    def expand(self, pcoll):
      paired_with_void_type = typehints.Tuple[pcoll.element_type, Any]
      return (pcoll
              | ('%s:PairWithVoid' % self.label >> core.Map(lambda x: (x, None))
                 .with_output_types(paired_with_void_type))
              | core.CombinePerKey(CountCombineFn()))


@with_input_types(Any)
@with_output_types(int)
class CountCombineFn(core.CombineFn):
  """CombineFn for computing PCollection size."""

  def create_accumulator(self):
    return 0

  def add_input(self, accumulator, element):
    return accumulator + 1

  def add_inputs(self, accumulator, elements):
    return accumulator + len(list(elements))

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


class Top(object):
  """Combiners for obtaining extremal elements."""
  # pylint: disable=no-self-argument

  class Of(ptransform.PTransform):
    """Obtain a list of the compare-most N elements in a PCollection.

    This transform will retrieve the n greatest elements in the PCollection
    to which it is applied, where "greatest" is determined by the comparator
    function supplied as the compare argument.
    """

    def _py2__init__(self, n, compare=None, *args, **kwargs):
      """Initializer.

      compare should be an implementation of "a < b" taking at least two
      arguments (a and b). Additional arguments and side inputs specified in
      the apply call become additional arguments to the comparator. Defaults to
      the natural ordering of the elements.
      The arguments 'key' and 'reverse' may instead be passed as keyword
      arguments, and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        compare: as described above.
        *args: as described above.
        **kwargs: as described above.
      """
      if compare:
        warnings.warn('Compare not available in Python 3, use key instead.',
                      DeprecationWarning)
      self._n = n
      self._compare = compare
      self._key = kwargs.pop('key', None)
      self._reverse = kwargs.pop('reverse', False)
      self._args = args
      self._kwargs = kwargs

    def _py3__init__(self, n, **kwargs):
      """Creates a global Top operation.

      The arguments 'key' and 'reverse' may be passed as keyword arguments,
      and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        **kwargs: may contain 'key' and/or 'reverse'
      """
      unknown_kwargs = set(kwargs.keys()) - set(['key', 'reverse'])
      if unknown_kwargs:
        raise ValueError(
            'Unknown keyword arguments: ' + ', '.join(unknown_kwargs))
      self._py2__init__(n, None, **kwargs)

    # Python 3 sort does not accept a comparison operator, and nor do we.
    if sys.version_info[0] < 3:
      __init__ = _py2__init__
    else:
      __init__ = _py3__init__

    def default_label(self):
      return 'Top(%d)' % self._n

    def expand(self, pcoll):
      compare = self._compare
      if (not self._args and not self._kwargs and
          pcoll.windowing.is_default()):
        if self._reverse:
          if compare is None or compare is operator.lt:
            compare = operator.gt
          else:
            original_compare = compare
            compare = lambda a, b: original_compare(b, a)
        # This is a more efficient global algorithm.
        top_per_bundle = pcoll | core.ParDo(
            _TopPerBundle(self._n, compare, self._key))
        # If pcoll is empty, we can't guerentee that top_per_bundle
        # won't be empty, so inject at least one empty accumulator
        # so that downstream is guerenteed to produce non-empty output.
        empty_bundle = pcoll.pipeline | core.Create([(None, [])])
        return (
            (top_per_bundle, empty_bundle) | core.Flatten()
            | core.GroupByKey()
            | core.ParDo(_MergeTopPerBundle(self._n, compare, self._key)))
      else:
        return pcoll | core.CombineGlobally(
            TopCombineFn(self._n, compare, self._key, self._reverse),
            *self._args, **self._kwargs)

  class PerKey(ptransform.PTransform):
    """Identifies the compare-most N elements associated with each key.

    This transform will produce a PCollection mapping unique keys in the input
    PCollection to the n greatest elements with which they are associated, where
    "greatest" is determined by the comparator function supplied as the compare
    argument in the initializer.
    """
    def _py2__init__(self, n, compare=None, *args, **kwargs):
      """Initializer.

      compare should be an implementation of "a < b" taking at least two
      arguments (a and b). Additional arguments and side inputs specified in
      the apply call become additional arguments to the comparator.  Defaults to
      the natural ordering of the elements.

      The arguments 'key' and 'reverse' may instead be passed as keyword
      arguments, and have the same meaning as for Python's sort functions.

      Args:
        n: number of elements to extract from input.
        compare: as described above.
        *args: as described above.
        **kwargs: as described above.
      """
      if compare:
        warnings.warn('Compare not available in Python 3, use key instead.',
                      DeprecationWarning)
      self._n = n
      self._compare = compare
      self._key = kwargs.pop('key', None)
      self._reverse = kwargs.pop('reverse', False)
      self._args = args
      self._kwargs = kwargs

    def _py3__init__(self, n, **kwargs):
      """Creates a per-key Top operation.

      The arguments 'key' and 'reverse' may be passed as keyword arguments,
      and have the same meaning as for Python's sort functions.

      Args:
        pcoll: PCollection to process.
        n: number of elements to extract from pcoll.
        **kwargs: may contain 'key' and/or 'reverse'
      """
      unknown_kwargs = set(kwargs.keys()) - set(['key', 'reverse'])
      if unknown_kwargs:
        raise ValueError(
            'Unknown keyword arguments: ' + ', '.join(unknown_kwargs))
      self._py2__init__(n, None, **kwargs)

    # Python 3 sort does not accept a comparison operator, and nor do we.
    if sys.version_info[0] < 3:
      __init__ = _py2__init__
    else:
      __init__ = _py3__init__

    def default_label(self):
      return 'TopPerKey(%d)' % self._n

    def expand(self, pcoll):
      """Expands the transform.

      Raises TypeCheckError: If the output type of the input PCollection is not
      compatible with Tuple[A, B].

      Args:
        pcoll: PCollection to process

      Returns:
        the PCollection containing the result.
      """
      return pcoll | core.CombinePerKey(
          TopCombineFn(self._n, self._compare, self._key, self._reverse),
          *self._args, **self._kwargs)

  @staticmethod
  @ptransform.ptransform_fn
  def Largest(pcoll, n):
    """Obtain a list of the greatest N elements in a PCollection."""
    return pcoll | Top.Of(n)

  @staticmethod
  @ptransform.ptransform_fn
  def Smallest(pcoll, n):
    """Obtain a list of the least N elements in a PCollection."""
    return pcoll | Top.Of(n, reverse=True)

  @staticmethod
  @ptransform.ptransform_fn
  def LargestPerKey(pcoll, n):
    """Identifies the N greatest elements associated with each key."""
    return pcoll | Top.PerKey(n)

  @staticmethod
  @ptransform.ptransform_fn
  def SmallestPerKey(pcoll, n, reverse=True):
    """Identifies the N least elements associated with each key."""
    return pcoll | Top.PerKey(n, reverse=True)


@with_input_types(T)
@with_output_types(Tuple[None, List[T]])
class _TopPerBundle(core.DoFn):
  def __init__(self, n, less_than, key):
    self._n = n
    self._less_than = None if less_than is operator.le else less_than
    self._key = key

  def start_bundle(self):
    self._heap = []

  def process(self, element):
    if self._less_than or self._key:
      element = cy_combiners.ComparableValue(
          element, self._less_than, self._key)
    if len(self._heap) < self._n:
      heapq.heappush(self._heap, element)
    else:
      heapq.heappushpop(self._heap, element)

  def finish_bundle(self):
    # Though sorting here results in more total work, this allows us to
    # skip most elements in the reducer.
    # Essentially, given s map bundles, we are trading about O(sn) compares in
    # the (single) reducer for O(sn log n) compares across all mappers.
    self._heap.sort()

    # Unwrap to avoid serialization via pickle.
    if self._less_than or self._key:
      yield window.GlobalWindows.windowed_value(
          (None, [wrapper.value for wrapper in self._heap]))
    else:
      yield window.GlobalWindows.windowed_value(
          (None, self._heap))


@with_input_types(Tuple[None, Iterable[List[T]]])
@with_output_types(List[T])
class _MergeTopPerBundle(core.DoFn):
  def __init__(self, n, less_than, key):
    self._n = n
    self._less_than = None if less_than is operator.lt else less_than
    self._key = key

  def process(self, key_and_bundles):
    _, bundles = key_and_bundles
    heap = []
    for bundle in bundles:
      if not heap:
        if self._less_than or self._key:
          heap = [
              cy_combiners.ComparableValue(element, self._less_than, self._key)
              for element in bundle]
        else:
          heap = bundle
        continue
      for element in reversed(bundle):
        if self._less_than or self._key:
          element = cy_combiners.ComparableValue(
              element, self._less_than, self._key)
        if len(heap) < self._n:
          heapq.heappush(heap, element)
        elif element < heap[0]:
          # Because _TopPerBundle returns sorted lists, all other elements
          # will also be smaller.
          break
        else:
          heapq.heappushpop(heap, element)

    heap.sort()
    if self._less_than or self._key:
      yield [wrapper.value for wrapper in reversed(heap)]
    else:
      yield heap[::-1]


@with_input_types(T)
@with_output_types(List[T])
class TopCombineFn(core.CombineFn):
  """CombineFn doing the combining for all of the Top transforms.

  This CombineFn uses a key or comparison operator to rank the elements.

  Args:
    compare: (optional) an implementation of "a < b" taking at least two
        arguments (a and b). Additional arguments and side inputs specified
        in the apply call become additional arguments to the comparator.
    key: (optional) a mapping of elements to a comparable key, similar to
        the key argument of Python's sorting methods.
    reverse: (optional) whether to order things smallest to largest, rather
        than largest to smallest
  """

  # TODO(robertwb): For Python 3, remove compare and only keep key.
  def __init__(self, n, compare=None, key=None, reverse=False):
    self._n = n

    if compare is operator.lt:
      compare = None
    elif compare is operator.gt:
      compare = None
      reverse = not reverse

    if compare:
      self._compare = (
          (lambda a, b, *args, **kwargs: not compare(a, b, *args, **kwargs))
          if reverse
          else compare)
    else:
      self._compare = operator.gt if reverse else operator.lt

    self._less_than = None
    self._key = key

  def _hydrated_heap(self, heap):
    if heap:
      first = heap[0]
      if isinstance(first, cy_combiners.ComparableValue):
        if first.requires_hydration:
          assert self._less_than is not None
          for comparable in heap:
            assert comparable.requires_hydration
            comparable.hydrate(self._less_than, self._key)
            assert not comparable.requires_hydration
          return heap
        else:
          return heap
      else:
        assert self._less_than is not None
        return [
            cy_combiners.ComparableValue(element, self._less_than, self._key)
            for element in heap
        ]
    else:
      return heap

  def display_data(self):
    return {'n': self._n,
            'compare': DisplayDataItem(self._compare.__name__
                                       if hasattr(self._compare, '__name__')
                                       else self._compare.__class__.__name__)
                       .drop_if_none()}

  # The accumulator type is a tuple
  # (bool, Union[List[T], List[ComparableValue[T]])
  # where the boolean indicates whether the second slot contains a List of T
  # (False) or List of ComparableValue[T] (True). In either case, the List
  # maintains heap invariance. When the contents of the List are
  # ComparableValue[T] they either all 'requires_hydration' or none do.
  # This accumulator representation allows us to minimize the data encoding
  # overheads. Creation of ComparableValues is elided for performance reasons
  # when there is no need for complicated comparison functions.
  def create_accumulator(self, *args, **kwargs):
    return (False, [])

  def add_input(self, accumulator, element, *args, **kwargs):
    # Caching to avoid paying the price of variadic expansion of args / kwargs
    # when it's not needed (for the 'if' case below).
    if self._less_than is None:
      if args or kwargs:
        self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
      else:
        self._less_than = self._compare

    holds_comparables, heap = accumulator
    if self._less_than is not operator.lt or self._key:
      heap = self._hydrated_heap(heap)
      holds_comparables = True
    else:
      assert not holds_comparables

    comparable = (
        cy_combiners.ComparableValue(element, self._less_than, self._key)
        if holds_comparables else element)

    if len(heap) < self._n:
      heapq.heappush(heap, comparable)
    else:
      heapq.heappushpop(heap, comparable)
    return (holds_comparables, heap)

  def merge_accumulators(self, accumulators, *args, **kwargs):
    if args or kwargs:
      self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
      add_input = lambda accumulator, element: self.add_input(
          accumulator, element, *args, **kwargs)
    else:
      self._less_than = self._compare
      add_input = self.add_input

    result_heap = None
    holds_comparables = None
    for accumulator in accumulators:
      holds_comparables, heap = accumulator
      if self._less_than is not operator.lt or self._key:
        heap = self._hydrated_heap(heap)
        holds_comparables = True
      else:
        assert not holds_comparables

      if result_heap is None:
        result_heap = heap
      else:
        for comparable in heap:
          _, result_heap = add_input(
              (holds_comparables, result_heap),
              comparable.value if holds_comparables else comparable)

    assert result_heap is not None and holds_comparables is not None
    return (holds_comparables, result_heap)

  def compact(self, accumulator, *args, **kwargs):
    holds_comparables, heap = accumulator
    # Unwrap to avoid serialization via pickle.
    if holds_comparables:
      return (False, [comparable.value for comparable in heap])
    else:
      return accumulator

  def extract_output(self, accumulator, *args, **kwargs):
    if args or kwargs:
      self._less_than = lambda a, b: self._compare(a, b, *args, **kwargs)
    else:
      self._less_than = self._compare

    holds_comparables, heap = accumulator
    if self._less_than is not operator.lt or self._key:
      if not holds_comparables:
        heap = self._hydrated_heap(heap)
        holds_comparables = True
    else:
      assert not holds_comparables

    assert len(heap) <= self._n
    heap.sort(reverse=True)
    return [
        comparable.value if holds_comparables else comparable
        for comparable in heap
    ]


class Largest(TopCombineFn):
  def default_label(self):
    return 'Largest(%s)' % self._n


class Smallest(TopCombineFn):

  def __init__(self, n):
    super(Smallest, self).__init__(n, reverse=True)

  def default_label(self):
    return 'Smallest(%s)' % self._n


class Sample(object):
  """Combiners for sampling n elements without replacement."""
  # pylint: disable=no-self-argument

  class FixedSizeGlobally(ptransform.PTransform):
    """Sample n elements from the input PCollection without replacement."""

    def __init__(self, n):
      self._n = n

    def expand(self, pcoll):
      return pcoll | core.CombineGlobally(SampleCombineFn(self._n))

    def display_data(self):
      return {'n': self._n}

    def default_label(self):
      return 'FixedSizeGlobally(%d)' % self._n

  class FixedSizePerKey(ptransform.PTransform):
    """Sample n elements associated with each key without replacement."""

    def __init__(self, n):
      self._n = n

    def expand(self, pcoll):
      return pcoll | core.CombinePerKey(SampleCombineFn(self._n))

    def display_data(self):
      return {'n': self._n}

    def default_label(self):
      return 'FixedSizePerKey(%d)' % self._n


@with_input_types(T)
@with_output_types(List[T])
class SampleCombineFn(core.CombineFn):
  """CombineFn for all Sample transforms."""

  def __init__(self, n):
    super(SampleCombineFn, self).__init__()
    # Most of this combiner's work is done by a TopCombineFn. We could just
    # subclass TopCombineFn to make this class, but since sampling is not
    # really a kind of Top operation, we use a TopCombineFn instance as a
    # helper instead.
    self._top_combiner = TopCombineFn(n)

  def create_accumulator(self):
    return self._top_combiner.create_accumulator()

  def add_input(self, heap, element):
    # Before passing elements to the Top combiner, we pair them with random
    # numbers. The elements with the n largest random number "keys" will be
    # selected for the output.
    return self._top_combiner.add_input(heap, (random.random(), element))

  def merge_accumulators(self, heaps):
    return self._top_combiner.merge_accumulators(heaps)

  def compact(self, heap):
    return self._top_combiner.compact(heap)

  def extract_output(self, heap):
    # Here we strip off the random number keys we added in add_input.
    return [e for _, e in self._top_combiner.extract_output(heap)]


class _TupleCombineFnBase(core.CombineFn):

  def __init__(self, *combiners):
    self._combiners = [core.CombineFn.maybe_from_callable(c) for c in combiners]
    self._named_combiners = combiners

  def display_data(self):
    combiners = [c.__name__ if hasattr(c, '__name__') else c.__class__.__name__
                 for c in self._named_combiners]
    return {'combiners': str(combiners)}

  def create_accumulator(self):
    return [c.create_accumulator() for c in self._combiners]

  def merge_accumulators(self, accumulators):
    return [c.merge_accumulators(a)
            for c, a in zip(self._combiners, zip(*accumulators))]

  def compact(self, accumulator):
    return [c.compact(a) for c, a in zip(self._combiners, accumulator)]

  def extract_output(self, accumulator):
    return tuple([c.extract_output(a)
                  for c, a in zip(self._combiners, accumulator)])


class TupleCombineFn(_TupleCombineFnBase):
  """A combiner for combining tuples via a tuple of combiners.

  Takes as input a tuple of N CombineFns and combines N-tuples by
  combining the k-th element of each tuple with the k-th CombineFn,
  outputting a new N-tuple of combined values.
  """

  def add_input(self, accumulator, element):
    return [c.add_input(a, e)
            for c, a, e in zip(self._combiners, accumulator, element)]

  def with_common_input(self):
    return SingleInputTupleCombineFn(*self._combiners)


class SingleInputTupleCombineFn(_TupleCombineFnBase):
  """A combiner for combining a single value via a tuple of combiners.

  Takes as input a tuple of N CombineFns and combines elements by
  applying each CombineFn to each input, producing an N-tuple of
  the outputs corresponding to each of the N CombineFn's outputs.
  """

  def add_input(self, accumulator, element):
    return [c.add_input(a, element)
            for c, a in zip(self._combiners, accumulator)]


class ToList(ptransform.PTransform):
  """A global CombineFn that condenses a PCollection into a single list."""

  def __init__(self, label='ToList'):  # pylint: disable=useless-super-delegation
    super(ToList, self).__init__(label)

  def expand(self, pcoll):
    return pcoll | self.label >> core.CombineGlobally(ToListCombineFn())


@with_input_types(T)
@with_output_types(List[T])
class ToListCombineFn(core.CombineFn):
  """CombineFn for to_list."""

  def create_accumulator(self):
    return []

  def add_input(self, accumulator, element):
    accumulator.append(element)
    return accumulator

  def merge_accumulators(self, accumulators):
    return sum(accumulators, [])

  def extract_output(self, accumulator):
    return accumulator


class ToDict(ptransform.PTransform):
  """A global CombineFn that condenses a PCollection into a single dict.

  PCollections should consist of 2-tuples, notionally (key, value) pairs.
  If multiple values are associated with the same key, only one of the values
  will be present in the resulting dict.
  """

  def __init__(self, label='ToDict'):  # pylint: disable=useless-super-delegation
    super(ToDict, self).__init__(label)

  def expand(self, pcoll):
    return pcoll | self.label >> core.CombineGlobally(ToDictCombineFn())


@with_input_types(Tuple[K, V])
@with_output_types(Dict[K, V])
class ToDictCombineFn(core.CombineFn):
  """CombineFn for to_dict."""

  def create_accumulator(self):
    return dict()

  def add_input(self, accumulator, element):
    key, value = element
    accumulator[key] = value
    return accumulator

  def merge_accumulators(self, accumulators):
    result = dict()
    for a in accumulators:
      result.update(a)
    return result

  def extract_output(self, accumulator):
    return accumulator


class _CurriedFn(core.CombineFn):
  """Wrapped CombineFn with extra arguments."""

  def __init__(self, fn, args, kwargs):
    self.fn = fn
    self.args = args
    self.kwargs = kwargs

  def create_accumulator(self):
    return self.fn.create_accumulator(*self.args, **self.kwargs)

  def add_input(self, accumulator, element):
    return self.fn.add_input(accumulator, element, *self.args, **self.kwargs)

  def merge_accumulators(self, accumulators):
    return self.fn.merge_accumulators(accumulators, *self.args, **self.kwargs)

  def compact(self, accumulator):
    return self.fn.compact(accumulator, *self.args, **self.kwargs)

  def extract_output(self, accumulator):
    return self.fn.extract_output(accumulator, *self.args, **self.kwargs)

  def apply(self, elements):
    return self.fn.apply(elements, *self.args, **self.kwargs)


def curry_combine_fn(fn, args, kwargs):
  if not args and not kwargs:
    return fn
  else:
    return _CurriedFn(fn, args, kwargs)


class PhasedCombineFnExecutor(object):
  """Executor for phases of combine operations."""

  def __init__(self, phase, fn, args, kwargs):

    self.combine_fn = curry_combine_fn(fn, args, kwargs)

    if phase == 'all':
      self.apply = self.full_combine
    elif phase == 'add':
      self.apply = self.add_only
    elif phase == 'merge':
      self.apply = self.merge_only
    elif phase == 'extract':
      self.apply = self.extract_only
    else:
      raise ValueError('Unexpected phase: %s' % phase)

  def full_combine(self, elements):
    return self.combine_fn.apply(elements)

  def add_only(self, elements):
    return self.combine_fn.add_inputs(
        self.combine_fn.create_accumulator(), elements)

  def merge_only(self, accumulators):
    return self.combine_fn.merge_accumulators(accumulators)

  def extract_only(self, accumulator):
    return self.combine_fn.extract_output(accumulator)


class Latest(object):
  """Combiners for computing the latest element"""

  @with_input_types(T)
  @with_output_types(T)
  class Globally(ptransform.PTransform):
    """Compute the element with the latest timestamp from a
    PCollection."""

    @staticmethod
    def add_timestamp(element, timestamp=core.DoFn.TimestampParam):
      return [(element, timestamp)]

    def expand(self, pcoll):
      return (pcoll
              | core.ParDo(self.add_timestamp)
              .with_output_types(Tuple[T, TimestampType])
              | core.CombineGlobally(LatestCombineFn()))

  @with_input_types(Tuple[K, V])
  @with_output_types(Tuple[K, V])
  class PerKey(ptransform.PTransform):
    """Compute elements with the latest timestamp for each key
    from a keyed PCollection"""

    @staticmethod
    def add_timestamp(element, timestamp=core.DoFn.TimestampParam):
      key, value = element
      return [(key, (value, timestamp))]

    def expand(self, pcoll):
      return (pcoll
              | core.ParDo(self.add_timestamp)
              .with_output_types(Tuple[K, Tuple[T, TimestampType]])
              | core.CombinePerKey(LatestCombineFn()))


@with_input_types(Tuple[T, TimestampType])
@with_output_types(T)
class LatestCombineFn(core.CombineFn):
  """CombineFn to get the element with the latest timestamp
  from a PCollection."""

  def create_accumulator(self):
    return (None, window.MIN_TIMESTAMP)

  def add_input(self, accumulator, element):
    if accumulator[1] > element[1]:
      return accumulator
    else:
      return element

  def merge_accumulators(self, accumulators):
    result = self.create_accumulator()
    for accumulator in accumulators:
      result = self.add_input(result, accumulator)
    return result

  def extract_output(self, accumulator):
    return accumulator[0]


class ApproximateQuantiles(object):
  """
  PTransfrom for getting getting data distributaion using approximate N-tile
  (e.g. quartiles, percentiles etc.) either globally or per-key.
  """

  @with_input_types(T)
  @with_output_types(List[T])
  class Globally(ptransform.PTransform):
    """
    PTransform takes PCollection and returns a list whose single value is list
    of approximate N-tiles of the input element of the input collection
    globally.

    Args:
      num_quantiles: number of N-tile requires.
      compare_fn: (optional) Comparator function which is an implementation
        of "a < b" taking at least two arguments (a and b). Which is later
        converted to key function as Python 3 does not support cmp.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
    """

    def __init__(self, num_quantiles, compare_fn=None, key=None):
      self.num_quantiles = num_quantiles
      self.compare_fn = compare_fn
      self.key = key

    def expand(self, pcoll):
      return pcoll | core.CombineGlobally(ApproximateQuantilesCombineFn.create(
          num_quantiles=self.num_quantiles, compare_fn=self.compare_fn,
          key=self.key
      ))

    def display_data(self):
      return {
          'num_quantiles': DisplayDataItem(self.num_quantiles,
                                           label="Quantile Count"),
          'compare_fn': DisplayDataItem(self.compare_fn.__class__,
                                        label='Record Comparer FN'),
          'compare_key': DisplayDataItem(self.key.__class__,
                                         label='Record Comparer Key')
      }

  @with_input_types(Tuple[K, V])
  @with_output_types(List[Tuple[K, V]])
  class PerKey(ptransform.PTransform):
    """
    PTransform takes PCollection of KV and returns a list based on each key
    whose single value is list of approximate N-tiles of the input element of
    the key.

    Args:
      num_quantiles: number of N-tile requires.
      compare_fn: (optional) Comparator function which is an implementation
        of "a < b" taking at least two arguments (a and b). Which is later
        converted to key function as Python 3 does not support cmp.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
    """

    def __init__(self, num_quantiles, compare_fn=None, key=None):
      self.num_quantiles = num_quantiles
      self.compare_fn = compare_fn
      self.key = key

    def expand(self, pcoll):
      return pcoll | core.CombinePerKey(ApproximateQuantilesCombineFn.create(
          num_quantiles=self.num_quantiles, compare_fn=self.compare_fn,
          key=self.key
      ))

    def display_data(self):
      return {
          'num_quantiles': DisplayDataItem(self.num_quantiles,
                                           label="Quantile Count"),
          'compare_fn': DisplayDataItem(self.compare_fn.__class__,
                                        label='Record Comparer FN'),
          'compare_key': DisplayDataItem(self.key.__class__,
                                         label='Record Comparer Key')
      }


class _QuantileBuffer(object):
  """A single buffer in the sense of the referenced algorithm."""

  def __init__(self, elements, level=None, weight=None):
    self.elements = elements
    self.level = level or 0
    self.weight = weight or 1

  def __lt__(self, other):
    self.elements < other.elements

  def sized_iterator(self):

    class QuantileBufferIterator(object):
      def __init__(self, elem, weight):
        self._iter = iter(elem)
        self.weight = weight

      def __iter__(self):
        return self

      def __next__(self):
        value = next(self._iter)
        return {'value': value, 'weight': self.weight}

      next = __next__  # For Python 2

    return QuantileBufferIterator(self.elements, self.weight)


class _QuantileState(object):
  """
  Compact summarization of a collection on which quantiles can be estimated.
  """
  min_val = None  # Holds smallest item in the list
  max_val = None  # Holds largest item in the list

  def __init__(self, buffer_size, num_buffers, unbuffered_elements, buffers):
    self.buffer_size = buffer_size
    self.num_buffers = num_buffers
    self.buffers = buffers

    # The algorithm requires that the manipulated buffers always be filled to
    # capacity to perform the collapse operation. This operation can be extended
    # to buffers of varying sizes by introducing the notion of fractional
    # weights, but it's easier to simply combine the remainders from all shards
    # into new, full buffers and then take them into account when computing the
    # final output.
    self.unbuffered_elements = unbuffered_elements

  def is_empty(self):
    """Check if the buffered & unbuffered elements are empty or not."""
    return not self.unbuffered_elements and not self.buffers

  def is_empty(self):
    return not self.unbuffered_elements and not self.buffers


class ApproximateQuantilesCombineFn(core.CombineFn):
  """
  This combiner gives an idea of the distribution of a collection of values
  using approximate N-tiles. The output of this combiner is the list of size of
  the number of qunatiles (num_quantiles), containing the input values of the
  minimum value item of the list, the intermediate values (n-tiles) and the
  maximum value item of the list, in the sort order provided via
  (compare_fn or key).

  If there are fewer values to combine than (num_quantiles), then the resulting
  list will contain all the values being combined, in sorted order.

  If not comparator (compare_fn or key) is provided, then the results are
  sorted in the natural order.

  The default error bound is (1 / N), though in practice the accuracy
  tends to be much better.

  To evaluate the quantiles, we use the "New Algorithm" described here:

  [MRL98] Manku, Rajagopalan & Lindsay, "Approximate Medians and other
  Quantiles in One Pass and with Limited Memory", Proc. 1998 ACM SIGMOD,
  Vol 27, No 2, p 426-435, June 1998.
  http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&rep=rep1
  &type=pdf

  Args:
    num_quantiles: Number of quantiles to produce. The size of the final output
      list, including the minimum and maximum, is num_qunatiles.
    buffer_size: The size of the buffers, corresponding to k in the referenced
      paper.
    num_buffers: The number of buffers, corresponding to b in the referenced
      paper.
    compare_fn: (optional) Comparator function which is an implementation
        of "a < b" taking at least two arguments (a and b). Which is later
        converted to key function as Python 3 does not support cmp.
    epsilon: (optional) The default error bound is `epsilon`, which holds as
      long as the number of elements is less than `MAX_NUM_ELEMENTS`.
      Specifically, if one considers the input as a sorted list x_1, ..., x_N,
      then the distance between the each exact quantile x_c and its
      approximation x_c' is bounded by `|c - c'| < epsilon * N`. Note that
      these errors are worst-case scenarios; in practice the accuracy tends to
      be much better.
    key: (optional) Key is  a mapping of elements to a comparable key, similar
      to the key argument of Python's sorting methods.
  """

  # For alternating between biasing up and down in the above even weight
  # collapse operation.
  offset_jitter = 0

  # The cost (in time and space) to compute quantiles to a given accuracy is a
  # function of the total number of elements in the data set. If an estimate is
  # not known or specified, we use this as an upper bound. If this is too low,
  # errors may exceed the requested tolerance; if too high, efficiency may be
  # non-optimal. The impact is logarithmic with respect to this value, so this
  # default should be fine for most uses.
  MAX_NUM_ELEMENTS = 1e9
  qs = None  # Refers to the QuantileState

  # TODO(mszb): For Python 3, remove compare and only keep key.
  def __init__(self, num_quantiles, buffer_size, num_buffers, compare_fn=None,
               key=None):
    if compare_fn:
      warnings.warn('Compare_fn not available in Python 3, use key instead.')

    self.compare_fn = compare_fn
    self.num_quantiles = num_quantiles
    self.buffer_size = buffer_size
    self.num_buffers = num_buffers
    self.key = key

  @staticmethod
  def create(num_quantiles, compare_fn=None, epsilon=None, key=None,
             max_num_elements=None):
    max_num_elements = max_num_elements or \
                       ApproximateQuantilesCombineFn.MAX_NUM_ELEMENTS
    if not epsilon:
      epsilon = 1.0 / num_quantiles

    if not key:
      if not compare_fn:
        try:
          compare_fn = cmp
        except NameError:
          compare_fn = lambda a, b: (a > b) - (a < b)  # for Python 3

      key = lambda elem: cmp_to_key(compare_fn)(elem)

    b = 2
    while (b - 2) * (1 << (b - 2)) < epsilon * max_num_elements:
      b = b + 1
    b = b - 1
    k = max(2, math.ceil(max_num_elements / float(1 << (b - 1))))
    return ApproximateQuantilesCombineFn(num_quantiles=num_quantiles,
                                         compare_fn=compare_fn, buffer_size=k,
                                         num_buffers=b,
                                         key=key)

  def add_unbuffered(self, elem):
    """
    Add a new buffer to the unbuffered list, creating a new buffer and
    collapsing if needed.
    """
    self.qs.unbuffered_elements.append(elem)
    if len(self.qs.unbuffered_elements) == self.qs.buffer_size:
      self.qs.unbuffered_elements.sort(key=self.key)
      heapq.heappush(self.qs.buffers,
                     _QuantileBuffer(elements=self.qs.unbuffered_elements))
      self.qs.unbuffered_elements = []
      self.collapse_if_needed()

  def offset(self, newWeight):
    """
    If the weight is even, we must round up or down. Alternate between these
    two options to avoid a bias.
    """
    if newWeight % 2 == 1:
      return (newWeight + 1) / 2
    else:
      self.offset_jitter = 2 - self.offset_jitter
      return (newWeight + self.offset_jitter) / 2

  def collapse(self, buffers):
    new_level = 0
    new_weight = 0
    for buffer_elem in buffers:
      # As presented in the paper, there should always be at least two
      # buffers of the same (minimal) level to collapse, but it is possible
      # to violate this condition when combining buffers from independently
      # computed shards.  If they differ we take the max.
      new_level = max([new_level, buffer_elem.level + 1])
      new_weight = new_weight + buffer_elem.weight
    new_elements = self.interpolate(buffers, self.buffer_size, new_weight,
                                    self.offset(new_weight))
    return _QuantileBuffer(new_elements, new_level, new_weight)

  def collapse_if_needed(self):
    while len(self.qs.buffers) > self.num_buffers:
      toCollapse = []
      toCollapse.append(heapq.heappop(self.qs.buffers))
      toCollapse.append(heapq.heappop(self.qs.buffers))
      minLevel = toCollapse[1].level

      while len(self.qs.buffers) > 0 and self.qs.buffers[0].level == minLevel:
        toCollapse.append(heapq.heappop(self.qs.buffers))

      heapq.heappush(self.qs.buffers, self.collapse(toCollapse))

  def interpolate(self, i_buffers, count, step, offset):
    """
    Emulates taking the ordered union of all elements in buffers, repeated
    according to their weight, and picking out the (k * step + offset)-th
    elements of this list for `0 <= k < count`.
    """

    iterators = []
    new_elements = []
    compare_key = lambda x: self.key(x['value'])
    for buffer_elem in i_buffers:
      iterators.append(buffer_elem.sized_iterator())

    # Python 3 `heapq.merge` support key comparison and returns an iterator &
    # does not pull the data into memory all at once. Python 2 does not
    # support comparison on its `heapq.merge` api, so we use the itertools
    # which takes the `key` function for comparison and creates an iterator
    # from it.
    if sys.version_info[0] < 3:
      sorted_elem = iter(
          sorted(itertools.chain.from_iterable(iterators), key=compare_key))
    else:
      sorted_elem = heapq.merge(*iterators, key=compare_key)

    weighted_element = next(sorted_elem)
    current = weighted_element['weight']
    j = 0
    while j < count:
      target = j * step + offset
      j = j + 1
      try:
        while current <= target:
          weighted_element = next(sorted_elem)
          current = current + weighted_element['weight']
      except StopIteration:
        pass
      new_elements.append(weighted_element['value'])
    return new_elements

  def create_accumulator(self):
    self.qs = _QuantileState(buffer_size=self.buffer_size,
                             num_buffers=self.num_buffers,
                             unbuffered_elements=[], buffers=[])
    return self.qs

  def add_input(self, quantile_state, element):
    """Add a new element to the collection being summarized by quntile state."""
    if quantile_state.is_empty():
      quantile_state.min_val = quantile_state.max_val = element
    elif self.compare_fn(element, quantile_state.min_val) < 0:
      quantile_state.min_val = element
    elif self.compare_fn(element, quantile_state.max_val) > 0:
      quantile_state.max_val = element
    self.add_unbuffered(elem=element)
    return self.qs

  def merge_accumulators(self, accumulators):
    """
    Merges all the accumulators (quantile state) as one.
    """
    if not self.qs:
      # create empty accumulator if its not available.
      self.create_accumulator()

    for accumulator in accumulators:
      if accumulator.is_empty():
        continue
      if not self.qs.min_val or self.compare_fn(accumulator.min_val,
                                                self.qs.min_val) < 0:
        self.qs.min_val = accumulator.min_val
      if not self.qs.max_val or self.compare_fn(accumulator.max_val,
                                                self.qs.max_val) > 0:
        self.qs.max_val = accumulator.max_val

      for unbuffered_element in accumulator.unbuffered_elements:
        self.add_unbuffered(unbuffered_element)

      self.qs.buffers.extend(accumulator.buffers)
    self.collapse_if_needed()
    return self.qs

  def extract_output(self, accumulator):
    """
    Outputs num_quantiles elements consisting of the minimum, maximum, and
    num_quantiles - 2 evenly spaced intermediate elements. Returns the empty
    list if no elements have been added.
    """
    if accumulator.is_empty():
      return []

    all_elems = accumulator.buffers
    total_count = len(accumulator.unbuffered_elements)
    for buffer_elem in all_elems:
      total_count = total_count + accumulator.buffer_size * buffer_elem.weight

    if accumulator.unbuffered_elements:
      accumulator.unbuffered_elements.sort(key=self.key)
      all_elems.append(_QuantileBuffer(accumulator.unbuffered_elements))

    step = 1.0 * total_count / (self.num_quantiles - 1)
    offset = (1.0 * total_count - 1) / (self.num_quantiles - 1)

    quantiles = [accumulator.min_val]
    quantiles.extend(
        self.interpolate(all_elems, self.num_quantiles - 2, step, offset))
    quantiles.append(accumulator.max_val)
    return quantiles
