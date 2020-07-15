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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import copy
import heapq
import operator
import random
import sys
import warnings
from builtins import object
from builtins import zip
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Set
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
    'Count', 'Mean', 'Sample', 'Top', 'ToDict', 'ToList', 'ToSet', 'Latest'
]

# Type variables
T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
TimestampType = Union[int, float, Timestamp, Duration]


class CombinerWithoutDefaults(ptransform.PTransform):
  """Parent class to inherit  without_defaults to built-in Combiners"""
  def __init__(self, has_defaults=True):
    self.has_defaults = has_defaults

  def with_defaults(self, has_defaults=True):
    new = copy.copy(self)
    new.has_defaults = has_defaults
    return new

  def without_defaults(self):
    return self.with_defaults(False)


class Mean(object):
  """Combiners for computing arithmetic means of elements."""
  class Globally(CombinerWithoutDefaults):
    """combiners.Mean.Globally computes the arithmetic mean of the elements."""
    def expand(self, pcoll):
      if self.has_defaults:
        return pcoll | core.CombineGlobally(MeanCombineFn())
      else:
        return pcoll | core.CombineGlobally(MeanCombineFn()).without_defaults()

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
  class Globally(CombinerWithoutDefaults):
    """combiners.Count.Globally counts the total number of elements."""
    def expand(self, pcoll):
      if self.has_defaults:
        return pcoll | core.CombineGlobally(CountCombineFn())
      else:
        return pcoll | core.CombineGlobally(CountCombineFn()).without_defaults()

  class PerKey(ptransform.PTransform):
    """combiners.Count.PerKey counts how many elements each unique key has."""
    def expand(self, pcoll):
      return pcoll | core.CombinePerKey(CountCombineFn())

  class PerElement(ptransform.PTransform):
    """combiners.Count.PerElement counts how many times each element occurs."""
    def expand(self, pcoll):
      paired_with_void_type = typehints.Tuple[pcoll.element_type, Any]
      output_type = typehints.KV[pcoll.element_type, int]
      return (
          pcoll
          | (
              '%s:PairWithVoid' % self.label >> core.Map(
                  lambda x: (x, None)).with_output_types(paired_with_void_type))
          | core.CombinePerKey(CountCombineFn()).with_output_types(output_type))


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

  class Of(CombinerWithoutDefaults):
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
      super(Top.Of, self).__init__()
      if compare:
        warnings.warn(
            'Compare not available in Python 3, use key instead.',
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
    # FIXME: mypy would handle this better if we placed the _py*__init__ funcs
    #  inside the if/else block below:
    if sys.version_info[0] < 3:
      __init__ = _py2__init__
    else:
      __init__ = _py3__init__  # type: ignore

    def default_label(self):
      return 'Top(%d)' % self._n

    def expand(self, pcoll):
      compare = self._compare
      if (not self._args and not self._kwargs and pcoll.windowing.is_default()):
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
        return ((top_per_bundle, empty_bundle) | core.Flatten()
                | core.GroupByKey()
                | core.ParDo(_MergeTopPerBundle(self._n, compare, self._key)))
      else:
        if self.has_defaults:
          return pcoll | core.CombineGlobally(
              TopCombineFn(self._n, compare, self._key, self._reverse),
              *self._args,
              **self._kwargs)
        else:
          return pcoll | core.CombineGlobally(
              TopCombineFn(self._n, compare, self._key, self._reverse),
              *self._args,
              **self._kwargs).without_defaults()

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
        warnings.warn(
            'Compare not available in Python 3, use key instead.',
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
      __init__ = _py3__init__  # type: ignore

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
          *self._args,
          **self._kwargs)

  @staticmethod
  @ptransform.ptransform_fn
  def Largest(pcoll, n, has_defaults=True):
    """Obtain a list of the greatest N elements in a PCollection."""
    if has_defaults:
      return pcoll | Top.Of(n)
    else:
      return pcoll | Top.Of(n).without_defaults()

  @staticmethod
  @ptransform.ptransform_fn
  def Smallest(pcoll, n, has_defaults=True):
    """Obtain a list of the least N elements in a PCollection."""
    if has_defaults:
      return pcoll | Top.Of(n, reverse=True)
    else:
      return pcoll | Top.Of(n, reverse=True).without_defaults()

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
      yield window.GlobalWindows.windowed_value((None, self._heap))


@with_input_types(Tuple[None, Iterable[List[T]]])
@with_output_types(List[T])
class _MergeTopPerBundle(core.DoFn):
  def __init__(self, n, less_than, key):
    self._n = n
    self._less_than = None if less_than is operator.lt else less_than
    self._key = key

  def process(self, key_and_bundles):
    _, bundles = key_and_bundles

    def push(hp, e):
      if len(hp) < self._n:
        heapq.heappush(hp, e)
        return False
      elif e < hp[0]:
        # Because _TopPerBundle returns sorted lists, all other elements
        # will also be smaller.
        return True
      else:
        heapq.heappushpop(hp, e)
        return False

    if self._less_than or self._key:
      heapc = []  # type: List[cy_combiners.ComparableValue]
      for bundle in bundles:
        if not heapc:
          heapc = [
              cy_combiners.ComparableValue(element, self._less_than, self._key)
              for element in bundle
          ]
          continue
        for element in reversed(bundle):
          if push(heapc,
                  cy_combiners.ComparableValue(element,
                                               self._less_than,
                                               self._key)):
            break
      heapc.sort()
      yield [wrapper.value for wrapper in reversed(heapc)]

    else:
      heap = []
      for bundle in bundles:
        if not heap:
          heap = bundle
          continue
        for element in reversed(bundle):
          if push(heap, element):
            break
      heap.sort()
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
      self._compare = ((
          lambda a, b, *args, **kwargs: not compare(a, b, *args, **kwargs))
                       if reverse else compare)
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
    return {
        'n': self._n,
        'compare': DisplayDataItem(
            self._compare.__name__ if hasattr(self._compare, '__name__') else
            self._compare.__class__.__name__).drop_if_none()
    }

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

  class FixedSizeGlobally(CombinerWithoutDefaults):
    """Sample n elements from the input PCollection without replacement."""
    def __init__(self, n):
      super(Sample.FixedSizeGlobally, self).__init__()
      self._n = n

    def expand(self, pcoll):
      if self.has_defaults:
        return pcoll | core.CombineGlobally(SampleCombineFn(self._n))
      else:
        return pcoll | core.CombineGlobally(SampleCombineFn(
            self._n)).without_defaults()

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
    combiners = [
        c.__name__ if hasattr(c, '__name__') else c.__class__.__name__
        for c in self._named_combiners
    ]
    return {'combiners': str(combiners)}

  def create_accumulator(self):
    return [c.create_accumulator() for c in self._combiners]

  def merge_accumulators(self, accumulators):
    return [
        c.merge_accumulators(a) for c,
        a in zip(self._combiners, zip(*accumulators))
    ]

  def compact(self, accumulator):
    return [c.compact(a) for c, a in zip(self._combiners, accumulator)]

  def extract_output(self, accumulator):
    return tuple(
        [c.extract_output(a) for c, a in zip(self._combiners, accumulator)])


class TupleCombineFn(_TupleCombineFnBase):
  """A combiner for combining tuples via a tuple of combiners.

  Takes as input a tuple of N CombineFns and combines N-tuples by
  combining the k-th element of each tuple with the k-th CombineFn,
  outputting a new N-tuple of combined values.
  """
  def add_input(self, accumulator, element):
    return [
        c.add_input(a, e) for c,
        a,
        e in zip(self._combiners, accumulator, element)
    ]

  def with_common_input(self):
    return SingleInputTupleCombineFn(*self._combiners)


class SingleInputTupleCombineFn(_TupleCombineFnBase):
  """A combiner for combining a single value via a tuple of combiners.

  Takes as input a tuple of N CombineFns and combines elements by
  applying each CombineFn to each input, producing an N-tuple of
  the outputs corresponding to each of the N CombineFn's outputs.
  """
  def add_input(self, accumulator, element):
    return [
        c.add_input(a, element) for c, a in zip(self._combiners, accumulator)
    ]


class ToList(CombinerWithoutDefaults):
  """A global CombineFn that condenses a PCollection into a single list."""
  def __init__(self, label='ToList'):  # pylint: disable=useless-super-delegation
    super(ToList, self).__init__(label)

  def expand(self, pcoll):
    if self.has_defaults:
      return pcoll | self.label >> core.CombineGlobally(ToListCombineFn())
    else:
      return pcoll | self.label >> core.CombineGlobally(
          ToListCombineFn()).without_defaults()


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


class ToDict(CombinerWithoutDefaults):
  """A global CombineFn that condenses a PCollection into a single dict.

  PCollections should consist of 2-tuples, notionally (key, value) pairs.
  If multiple values are associated with the same key, only one of the values
  will be present in the resulting dict.
  """
  def __init__(self, label='ToDict'):  # pylint: disable=useless-super-delegation
    super(ToDict, self).__init__(label)

  def expand(self, pcoll):
    if self.has_defaults:
      return pcoll | self.label >> core.CombineGlobally(ToDictCombineFn())
    else:
      return pcoll | self.label >> core.CombineGlobally(
          ToDictCombineFn()).without_defaults()


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


class ToSet(CombinerWithoutDefaults):
  """A global CombineFn that condenses a PCollection into a set."""
  def __init__(self, label='ToSet'):  # pylint: disable=useless-super-delegation
    super(ToSet, self).__init__(label)

  def expand(self, pcoll):
    if self.has_defaults:
      return pcoll | self.label >> core.CombineGlobally(ToSetCombineFn())
    else:
      return pcoll | self.label >> core.CombineGlobally(
          ToSetCombineFn()).without_defaults()


@with_input_types(T)
@with_output_types(Set[T])
class ToSetCombineFn(core.CombineFn):
  """CombineFn for ToSet."""
  def create_accumulator(self):
    return set()

  def add_input(self, accumulator, element):
    accumulator.add(element)
    return accumulator

  def merge_accumulators(self, accumulators):
    return set.union(*accumulators)

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
    elif phase == 'convert':
      self.apply = self.convert_to_accumulator
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

  def convert_to_accumulator(self, element):
    return self.combine_fn.add_input(
        self.combine_fn.create_accumulator(), element)


class Latest(object):
  """Combiners for computing the latest element"""
  @with_input_types(T)
  @with_output_types(T)
  class Globally(CombinerWithoutDefaults):
    """Compute the element with the latest timestamp from a
    PCollection."""
    @staticmethod
    def add_timestamp(element, timestamp=core.DoFn.TimestampParam):
      return [(element, timestamp)]

    def expand(self, pcoll):
      if self.has_defaults:
        return (
            pcoll
            | core.ParDo(self.add_timestamp).with_output_types(
                Tuple[T, TimestampType])  # type: ignore[misc]
            | core.CombineGlobally(LatestCombineFn()))
      else:
        return (
            pcoll
            | core.ParDo(self.add_timestamp).with_output_types(
                Tuple[T, TimestampType])  # type: ignore[misc]
            | core.CombineGlobally(LatestCombineFn()).without_defaults())

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
      return (
          pcoll
          | core.ParDo(self.add_timestamp).with_output_types(
              Tuple[K, Tuple[T, TimestampType]])  # type: ignore[misc]
          | core.CombinePerKey(LatestCombineFn()))


@with_input_types(Tuple[T, TimestampType])  # type: ignore[misc]
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
