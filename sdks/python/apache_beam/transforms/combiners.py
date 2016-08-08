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

import operator
import random

from apache_beam.transforms import core
from apache_beam.transforms import cy_combiners
from apache_beam.transforms import ptransform
from apache_beam.typehints import Any
from apache_beam.typehints import Dict
from apache_beam.typehints import KV
from apache_beam.typehints import List
from apache_beam.typehints import Tuple
from apache_beam.typehints import TypeVariable
from apache_beam.typehints import Union
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types


__all__ = [
    'Count',
    'Mean',
    'Sample',
    'Top',
    'ToDict',
    'ToList',
    ]

# Type variables
T = TypeVariable('T')
K = TypeVariable('K')
V = TypeVariable('V')


class Mean(object):
  """Combiners for computing arithmetic means of elements."""

  class Globally(ptransform.PTransform):
    """combiners.Mean.Globally computes the arithmetic mean of the elements."""

    def apply(self, pcoll):
      return pcoll | core.CombineGlobally(MeanCombineFn())

  class PerKey(ptransform.PTransform):
    """combiners.Mean.PerKey finds the means of the values for each key."""

    def apply(self, pcoll):
      return pcoll | core.CombinePerKey(MeanCombineFn())


# TODO(laolu): This type signature is overly restrictive. This should be
# more general.
@with_input_types(Union[float, int, long])
@with_output_types(float)
class MeanCombineFn(core.CombineFn):
  """CombineFn for computing an arithmetic mean."""

  def create_accumulator(self):
    return (0, 0)

  def add_input(self, (sum_, count), element):
    return sum_ + element, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, (sum_, count)):
    if count == 0:
      return float('NaN')
    return sum_ / float(count)

  def for_input_type(self, input_type):
    if input_type is int:
      return cy_combiners.MeanInt64Fn()
    elif input_type is float:
      return cy_combiners.MeanFloatFn()
    else:
      return self


class Count(object):
  """Combiners for counting elements."""

  class Globally(ptransform.PTransform):
    """combiners.Count.Globally counts the total number of elements."""

    def apply(self, pcoll):
      return pcoll | core.CombineGlobally(CountCombineFn())

  class PerKey(ptransform.PTransform):
    """combiners.Count.PerKey counts how many elements each unique key has."""

    def apply(self, pcoll):
      return pcoll | core.CombinePerKey(CountCombineFn())

  class PerElement(ptransform.PTransform):
    """combiners.Count.PerElement counts how many times each element occurs."""

    def apply(self, pcoll):
      paired_with_void_type = KV[pcoll.element_type, Any]
      return (pcoll
              | (core.Map('%s:PairWithVoid' % self.label, lambda x: (x, None))
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
    return accumulator + len(elements)

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


class Top(object):
  """Combiners for obtaining extremal elements."""
  # pylint: disable=no-self-argument

  @ptransform.ptransform_fn
  def Of(pcoll, n, compare=None, *args, **kwargs):
    """Obtain a list of the compare-most N elements in a PCollection.

    This transform will retrieve the n greatest elements in the PCollection
    to which it is applied, where "greatest" is determined by the comparator
    function supplied as the compare argument.

    compare should be an implementation of "a < b" taking at least two arguments
    (a and b). Additional arguments and side inputs specified in the apply call
    become additional arguments to the comparator.  Defaults to the natural
    ordering of the elements.

    The arguments 'key' and 'reverse' may instead be passed as keyword
    arguments, and have the same meaning as for Python's sort functions.

    Args:
      pcoll: PCollection to process.
      n: number of elements to extract from pcoll.
      compare: as described above.
      *args: as described above.
      **kwargs: as described above.
    """
    key = kwargs.pop('key', None)
    reverse = kwargs.pop('reverse', False)
    return pcoll | core.CombineGlobally(
        TopCombineFn(n, compare, key, reverse), *args, **kwargs)

  @ptransform.ptransform_fn
  def PerKey(pcoll, n, compare=None, *args, **kwargs):
    """Identifies the compare-most N elements associated with each key.

    This transform will produce a PCollection mapping unique keys in the input
    PCollection to the n greatest elements with which they are associated, where
    "greatest" is determined by the comparator function supplied as the compare
    argument.

    compare should be an implementation of "a < b" taking at least two arguments
    (a and b). Additional arguments and side inputs specified in the apply call
    become additional arguments to the comparator.  Defaults to the natural
    ordering of the elements.

    The arguments 'key' and 'reverse' may instead be passed as keyword
    arguments, and have the same meaning as for Python's sort functions.

    Args:
      pcoll: PCollection to process.
      n: number of elements to extract from pcoll.
      compare: as described above.
      *args: as described above.
      **kwargs: as described above.

    Raises:
      TypeCheckError: If the output type of the input PCollection is not
        compatible with KV[A, B].
    """
    key = kwargs.pop('key', None)
    reverse = kwargs.pop('reverse', False)
    return pcoll | core.CombinePerKey(
        TopCombineFn(n, compare, key, reverse), *args, **kwargs)

  @ptransform.ptransform_fn
  def Largest(pcoll, n):
    """Obtain a list of the greatest N elements in a PCollection."""
    return pcoll | Top.Of(n)

  @ptransform.ptransform_fn
  def Smallest(pcoll, n):
    """Obtain a list of the least N elements in a PCollection."""
    return pcoll | Top.Of(n, reverse=True)

  @ptransform.ptransform_fn
  def LargestPerKey(pcoll, n):
    """Identifies the N greatest elements associated with each key."""
    return pcoll | Top.PerKey(n)

  @ptransform.ptransform_fn
  def SmallestPerKey(pcoll, n, reverse=True):
    """Identifies the N least elements associated with each key."""
    return pcoll | Top.PerKey(n, reverse=True)


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

  _MIN_BUFFER_OVERSIZE = 100
  _MAX_BUFFER_OVERSIZE = 1000

  # TODO(robertwb): Allow taking a key rather than a compare.
  def __init__(self, n, compare=None, key=None, reverse=False):
    self._n = n
    self._buffer_size = max(
        min(2 * n, n + TopCombineFn._MAX_BUFFER_OVERSIZE),
        n + TopCombineFn._MIN_BUFFER_OVERSIZE)

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

    self._key_fn = key
    self._reverse = reverse

  def _sort_buffer(self, buffer, lt):
    if lt in (operator.gt, operator.lt):
      buffer.sort(key=self._key_fn, reverse=self._reverse)
    else:
      buffer.sort(cmp=lambda a, b: (not lt(a, b)) - (not lt(b, a)),
                  key=self._key_fn)

  # The accumulator type is a tuple (threshold, buffer), where threshold
  # is the smallest element [key] that could possibly be in the top n based
  # on the elements observed so far, and buffer is a (periodically sorted)
  # list of candidates of bounded size.

  def create_accumulator(self, *args, **kwargs):
    return None, []

  def add_input(self, accumulator, element, *args, **kwargs):
    if args or kwargs:
      lt = lambda a, b: self._compare(a, b, *args, **kwargs)
    else:
      lt = self._compare

    threshold, buffer = accumulator
    element_key = self._key_fn(element) if self._key_fn else element

    if len(buffer) < self._n:
      if not buffer:
        return element_key, [element]
      else:
        buffer.append(element)
        if lt(element_key, threshold):  # element_key < threshold
          return element_key, buffer
        else:
          return accumulator  # with mutated buffer
    elif lt(threshold, element_key):  # threshold < element_key
      buffer.append(element)
      if len(buffer) < self._buffer_size:
        return accumulator
      else:
        self._sort_buffer(buffer, lt)
        min_element = buffer[-self._n]
        threshold = self._key_fn(min_element) if self._key_fn else min_element
        return threshold, buffer[-self._n:]
    else:
      return accumulator

  def merge_accumulators(self, accumulators, *args, **kwargs):
    accumulators = list(accumulators)
    if args or kwargs:
      add_input = lambda accumulator, element: self.add_input(
          accumulator, element, *args, **kwargs)
    else:
      add_input = self.add_input

    total_accumulator = None
    for accumulator in accumulators:
      if total_accumulator is None:
        total_accumulator = accumulator
      else:
        for element in accumulator[1]:
          total_accumulator = add_input(total_accumulator, element)
    return total_accumulator

  def extract_output(self, accumulator, *args, **kwargs):
    if args or kwargs:
      lt = lambda a, b: self._compare(a, b, *args, **kwargs)
    else:
      lt = self._compare

    _, buffer = accumulator
    self._sort_buffer(buffer, lt)
    return buffer[:-self._n-1:-1]  # tail, reversed


class Largest(TopCombineFn):

  def __init__(self, n):
    super(Largest, self).__init__(n)

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

  @ptransform.ptransform_fn
  def FixedSizeGlobally(pcoll, n):
    return pcoll | core.CombineGlobally(SampleCombineFn(n))

  @ptransform.ptransform_fn
  def FixedSizePerKey(pcoll, n):
    return pcoll | core.CombinePerKey(SampleCombineFn(n))


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

  def extract_output(self, heap):
    # Here we strip off the random number keys we added in add_input.
    return [e for _, e in self._top_combiner.extract_output(heap)]


class _TupleCombineFnBase(core.CombineFn):

  def __init__(self, *combiners):
    self._combiners = [core.CombineFn.maybe_from_callable(c) for c in combiners]

  def create_accumulator(self):
    return [c.create_accumulator() for c in self._combiners]

  def merge_accumulators(self, accumulators):
    return [c.merge_accumulators(a)
            for c, a in zip(self._combiners, zip(*accumulators))]

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

  def __init__(self, label='ToList'):
    super(ToList, self).__init__(label)

  def apply(self, pcoll):
    return pcoll | core.CombineGlobally(self.label, ToListCombineFn())


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

  def __init__(self, label='ToDict'):
    super(ToDict, self).__init__(label)

  def apply(self, pcoll):
    return pcoll | core.CombineGlobally(self.label, ToDictCombineFn())


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


def curry_combine_fn(fn, args, kwargs):
  if not args and not kwargs:
    return fn

  else:

    class CurriedFn(core.CombineFn):
      """CombineFn that applies extra arguments."""

      def create_accumulator(self):
        return fn.create_accumulator(*args, **kwargs)

      def add_input(self, accumulator, element):
        return fn.add_input(accumulator, element, *args, **kwargs)

      def merge_accumulators(self, accumulators):
        return fn.merge_accumulators(accumulators, *args, **kwargs)

      def extract_output(self, accumulator):
        return fn.extract_output(accumulator, *args, **kwargs)

      def apply(self, elements):
        return fn.apply(elements, *args, **kwargs)

    return CurriedFn()


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

  def full_combine(self, elements):  # pylint: disable=invalid-name
    return self.combine_fn.apply(elements)

  def add_only(self, elements):  # pylint: disable=invalid-name
    return self.combine_fn.add_inputs(
        self.combine_fn.create_accumulator(), elements)

  def merge_only(self, accumulators):  # pylint: disable=invalid-name
    return self.combine_fn.merge_accumulators(accumulators)

  def extract_only(self, accumulator):  # pylint: disable=invalid-name
    return self.combine_fn.extract_output(accumulator)
