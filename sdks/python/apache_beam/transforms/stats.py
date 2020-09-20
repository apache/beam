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

"""This module has all statistic related transforms.

This ApproximateUnique class will be deprecated [1]. PLease look into using
HLLCount in the zetasketch extension module [2].

[1] https://lists.apache.org/thread.html/501605df5027567099b81f18c080469661fb426
4a002615fa1510502%40%3Cdev.beam.apache.org%3E
[2] https://beam.apache.org/releases/javadoc/2.16.0/org/apache/beam/sdk/extensio
ns/zetasketch/HllCount.html
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import hashlib
import heapq
import itertools
import logging
import math
import sys
import typing
from builtins import round
from typing import Any
from typing import Generic
from typing import Iterable
from typing import List
from typing import Sequence

from apache_beam import coders
from apache_beam import typehints
from apache_beam.transforms.core import *
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.ptransform import PTransform

__all__ = [
    'ApproximateQuantiles',
    'ApproximateUnique',
]

# Type variables
T = typing.TypeVar('T')
K = typing.TypeVar('K')
V = typing.TypeVar('V')


def _get_default_hash_fn():
  """Returns either murmurhash or md5 based on installation."""
  try:
    import mmh3  # pylint: disable=import-error

    def _mmh3_hash(value):
      # mmh3.hash64 returns two 64-bit unsigned integers
      return mmh3.hash64(value, seed=0, signed=False)[0]

    return _mmh3_hash

  except ImportError:
    logging.warning(
        'Couldn\'t find murmurhash. Install mmh3 for a faster implementation of'
        'ApproximateUnique.')

    def _md5_hash(value):
      # md5 is a 128-bit hash, so we truncate the hexdigest (string of 32
      # hexadecimal digits) to 16 digits and convert to int to get the 64-bit
      # integer fingerprint.
      return int(hashlib.md5(value).hexdigest()[:16], 16)

    return _md5_hash


class ApproximateUnique(object):
  """
  Hashes input elements and uses those to extrapolate the size of the entire
  set of hash values by assuming the rest of the hash values are as densely
  distributed as the sample space.
  """

  _NO_VALUE_ERR_MSG = 'Either size or error should be set. Received {}.'
  _MULTI_VALUE_ERR_MSG = 'Either size or error should be set. ' \
                         'Received {size = %s, error = %s}.'
  _INPUT_SIZE_ERR_MSG = 'ApproximateUnique needs a size >= 16 for an error ' \
                        '<= 0.50. In general, the estimation error is about ' \
                        '2 / sqrt(sample_size). Received {size = %s}.'
  _INPUT_ERROR_ERR_MSG = 'ApproximateUnique needs an estimation error ' \
                         'between 0.01 and 0.50. Received {error = %s}.'

  @staticmethod
  def parse_input_params(size=None, error=None):
    """
    Check if input params are valid and return sample size.

    :param size: an int not smaller than 16, which we would use to estimate
      number of unique values.
    :param error: max estimation error, which is a float between 0.01 and 0.50.
      If error is given, sample size will be calculated from error with
      _get_sample_size_from_est_error function.
    :return: sample size
    :raises:
      ValueError: If both size and error are given, or neither is given, or
      values are out of range.
    """

    if None not in (size, error):
      raise ValueError(ApproximateUnique._MULTI_VALUE_ERR_MSG % (size, error))
    elif size is None and error is None:
      raise ValueError(ApproximateUnique._NO_VALUE_ERR_MSG)
    elif size is not None:
      if not isinstance(size, int) or size < 16:
        raise ValueError(ApproximateUnique._INPUT_SIZE_ERR_MSG % (size))
      else:
        return size
    else:
      if error < 0.01 or error > 0.5:
        raise ValueError(ApproximateUnique._INPUT_ERROR_ERR_MSG % (error))
      else:
        return ApproximateUnique._get_sample_size_from_est_error(error)

  @staticmethod
  def _get_sample_size_from_est_error(est_err):
    """
    :return: sample size

    Calculate sample size from estimation error
    """
    #math.ceil in python2.7 returns a float, while it returns an int in python3.
    return int(math.ceil(4.0 / math.pow(est_err, 2.0)))

  @typehints.with_input_types(T)
  @typehints.with_output_types(int)
  class Globally(PTransform):
    """ Approximate.Globally approximate number of unique values"""
    def __init__(self, size=None, error=None):
      self._sample_size = ApproximateUnique.parse_input_params(size, error)

    def expand(self, pcoll):
      coder = coders.registry.get_coder(pcoll)
      return pcoll \
             | 'CountGlobalUniqueValues' \
             >> (CombineGlobally(ApproximateUniqueCombineFn(self._sample_size,
                                                            coder)))

  @typehints.with_input_types(typing.Tuple[K, V])
  @typehints.with_output_types(typing.Tuple[K, int])
  class PerKey(PTransform):
    """ Approximate.PerKey approximate number of unique values per key"""
    def __init__(self, size=None, error=None):
      self._sample_size = ApproximateUnique.parse_input_params(size, error)

    def expand(self, pcoll):
      coder = coders.registry.get_coder(pcoll)
      return pcoll \
             | 'CountPerKeyUniqueValues' \
             >> (CombinePerKey(ApproximateUniqueCombineFn(self._sample_size,
                                                          coder)))


class _LargestUnique(object):
  """
  An object to keep samples and calculate sample hash space. It is an
  accumulator of a combine function.
  """
  # We use unsigned 64-bit integer hashes.
  _HASH_SPACE_SIZE = 2.0**64

  def __init__(self, sample_size):
    self._sample_size = sample_size
    self._min_hash = 2.0**64
    self._sample_heap = []
    self._sample_set = set()

  def add(self, element):
    """
    :param an element from pcoll.
    :return: boolean type whether the value is in the heap

    Adds a value to the heap, returning whether the value is (large enough to
    be) in the heap.
    """
    if len(self._sample_heap) >= self._sample_size and element < self._min_hash:
      return False

    if element not in self._sample_set:
      self._sample_set.add(element)
      heapq.heappush(self._sample_heap, element)

      if len(self._sample_heap) > self._sample_size:
        temp = heapq.heappop(self._sample_heap)
        self._sample_set.remove(temp)
        self._min_hash = self._sample_heap[0]
      elif element < self._min_hash:
        self._min_hash = element
    return True

  def get_estimate(self):
    """
    :return: estimation count of unique values

    If heap size is smaller than sample size, just return heap size.
    Otherwise, takes into account the possibility of hash collisions,
    which become more likely than not for 2^32 distinct elements.
    Note that log(1+x) ~ x for small x, so for sampleSize << maxHash
    log(1 - sample_size/sample_space) / log(1 - 1/sample_space) ~ sample_size
    and hence estimate ~ sample_size * hash_space / sample_space
    as one would expect.

    Given sample_size / sample_space = est / hash_space
    est = sample_size * hash_space / sample_space

    Given above sample_size approximate,
    est = log1p(-sample_size/sample_space) / log1p(-1/sample_space)
      * hash_space / sample_space
    """
    if len(self._sample_heap) < self._sample_size:
      return len(self._sample_heap)
    else:
      sample_space_size = self._HASH_SPACE_SIZE - 1.0 * self._min_hash
      est = (
          math.log1p(-self._sample_size / sample_space_size) /
          math.log1p(-1 / sample_space_size) * self._HASH_SPACE_SIZE /
          sample_space_size)
      return round(est)


class ApproximateUniqueCombineFn(CombineFn):
  """
  ApproximateUniqueCombineFn computes an estimate of the number of
  unique values that were combined.
  """
  def __init__(self, sample_size, coder):
    self._sample_size = sample_size
    coder = coders.typecoders.registry.verify_deterministic(
        coder, 'ApproximateUniqueCombineFn')

    self._coder = coder
    self._hash_fn = _get_default_hash_fn()

  def create_accumulator(self, *args, **kwargs):
    return _LargestUnique(self._sample_size)

  def add_input(self, accumulator, element, *args, **kwargs):
    try:
      hashed_value = self._hash_fn(self._coder.encode(element))
      accumulator.add(hashed_value)
      return accumulator
    except Exception as e:
      raise RuntimeError("Runtime exception: %s" % e)

  # created an issue https://issues.apache.org/jira/browse/BEAM-7285 to speed up
  # merge process.
  def merge_accumulators(self, accumulators, *args, **kwargs):
    merged_accumulator = self.create_accumulator()
    for accumulator in accumulators:
      for i in accumulator._sample_heap:
        merged_accumulator.add(i)

    return merged_accumulator

  @staticmethod
  def extract_output(accumulator):
    return accumulator.get_estimate()

  def display_data(self):
    return {'sample_size': self._sample_size}


class ApproximateQuantiles(object):
  """
  PTransform for getting the idea of data distribution using approximate N-tile
  (e.g. quartiles, percentiles etc.) either globally or per-key.

  Examples:

    in: list(range(101)), num_quantiles=5

    out: [0, 25, 50, 75, 100]

    in: [(i, 1 if i<10 else 1e-5) for i in range(101)], num_quantiles=5,
      weighted=True

    out: [0, 2, 5, 7, 100]
  """
  @staticmethod
  def _display_data(num_quantiles, key, reverse, weighted):
    return {
        'num_quantiles': DisplayDataItem(num_quantiles, label='Quantile Count'),
        'key': DisplayDataItem(
            key.__name__
            if hasattr(key, '__name__') else key.__class__.__name__,
            label='Record Comparer Key'),
        'reverse': DisplayDataItem(str(reverse), label='Is Reversed'),
        'weighted': DisplayDataItem(str(weighted), label='Is Weighted')
    }

  @typehints.with_input_types(
      typehints.Union[typing.Sequence[T], typing.Tuple[T, float]])
  @typehints.with_output_types(typing.List[T])
  class Globally(PTransform):
    """
    PTransform takes PCollection and returns a list whose single value is
    approximate N-tiles of the input collection globally.

    Args:
      num_quantiles: number of elements in the resulting quantiles values list.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
      reverse: (optional) whether to order things smallest to largest, rather
        than largest to smallest.
      weighted: (optional) if set to True, the transform returns weighted
        quantiles. The input PCollection is then expected to contain tuples of
        input values with the corresponding weight.
    """
    def __init__(self, num_quantiles, key=None, reverse=False, weighted=False):
      self._num_quantiles = num_quantiles
      self._key = key
      self._reverse = reverse
      self._weighted = weighted

    def expand(self, pcoll):
      return pcoll | CombineGlobally(
          ApproximateQuantilesCombineFn.create(
              num_quantiles=self._num_quantiles,
              key=self._key,
              reverse=self._reverse,
              weighted=self._weighted))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self._num_quantiles,
          key=self._key,
          reverse=self._reverse,
          weighted=self._weighted)

  @typehints.with_input_types(
      typehints.Union[typing.Tuple[K, V],
                      typing.Tuple[K, typing.Tuple[V, float]]])
  @typehints.with_output_types(typing.Tuple[K, typing.List[V]])
  class PerKey(PTransform):
    """
    PTransform takes PCollection of KV and returns a list based on each key
    whose single value is list of approximate N-tiles of the input element of
    the key.

    Args:
      num_quantiles: number of elements in the resulting quantiles values list.
      key: (optional) Key is  a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
      reverse: (optional) whether to order things smallest to largest, rather
        than largest to smallest.
      weighted: (optional) if set to True, the transform returns weighted
        quantiles. The input PCollection is then expected to contain tuples of
        input values with the corresponding weight.
    """
    def __init__(self, num_quantiles, key=None, reverse=False, weighted=False):
      self._num_quantiles = num_quantiles
      self._key = key
      self._reverse = reverse
      self._weighted = weighted

    def expand(self, pcoll):
      return pcoll | CombinePerKey(
          ApproximateQuantilesCombineFn.create(
              num_quantiles=self._num_quantiles,
              key=self._key,
              reverse=self._reverse,
              weighted=self._weighted))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self._num_quantiles,
          key=self._key,
          reverse=self._reverse,
          weighted=self._weighted)


class _QuantileBuffer(Generic[T]):
  """A single buffer in the sense of the referenced algorithm.
  (see http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&rep=rep1
  &type=pdf and ApproximateQuantilesCombineFn for further information)"""
  def __init__(self, elements, weighted, level=0, weight=1):
    # type: (Sequence[T], bool, int, int) -> None
    # In case of weighted quantiles, elements are tuples of values and weights.
    self.elements = elements
    self.weighted = weighted
    self.level = level
    self.weight = weight

  def __lt__(self, other):
    if self.weighted:
      return [element[0] for element in self.elements
              ] < [element[0] for element in other.elements]
    else:
      return self.elements < other.elements

  def sized_iterator(self):
    class QuantileBufferIterator(object):
      def __init__(self, elem, weighted, weight):
        self._iter = iter(elem)
        self.weighted = weighted
        self.weight = weight

      def __iter__(self):
        return self

      def __next__(self):
        if self.weighted:
          return next(self._iter)
        else:
          value = next(self._iter)
          return (value, self.weight)

      next = __next__  # For Python 2

    return QuantileBufferIterator(self.elements, self.weighted, self.weight)


class _QuantileState(Generic[T]):
  """
  Compact summarization of a collection on which quantiles can be estimated.
  """
  min_val = None  # type: Any  # Holds smallest item in the list
  max_val = None  # type: Any  # Holds largest item in the list

  def __init__(self, buffer_size, num_buffers, unbuffered_elements, buffers):
    # type: (int, int, List[Any], List[_QuantileBuffer[T]]) -> None
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
    # type: () -> bool

    """Check if the buffered & unbuffered elements are empty or not."""
    return not self.unbuffered_elements and not self.buffers


class ApproximateQuantilesCombineFn(CombineFn, Generic[T]):
  """
  This combiner gives an idea of the distribution of a collection of values
  using approximate N-tiles. The output of this combiner is the list of size of
  the number of quantiles (num_quantiles), containing the input values of the
  minimum value item of the list, the intermediate values (n-tiles) and the
  maximum value item of the list, in the sort order provided via key (similar
  to the key argument of Python's sorting methods).

  If there are fewer values to combine than the number of quantile
  (num_quantiles), then the resulting list will contain all the values being
  combined, in sorted order.

  If no `key` is provided, then the results are sorted in the natural order.

  To evaluate the quantiles, we use the "New Algorithm" described here:

  [MRL98] Manku, Rajagopalan & Lindsay, "Approximate Medians and other
  Quantiles in One Pass and with Limited Memory", Proc. 1998 ACM SIGMOD,
  Vol 27, No 2, p 426-435, June 1998.
  http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&rep=rep1
  &type=pdf

  The default error bound is (1 / N) for uniformly distributed data and
  min(1e-2, 1 / N) for weighted case, though in practice the accuracy tends to
  be much better.

  Args:
    num_quantiles: Number of quantiles to produce. It is the size of the final
      output list, including the mininum and maximum value items.
    buffer_size: The size of the buffers, corresponding to k in the referenced
      paper.
    num_buffers: The number of buffers, corresponding to b in the referenced
      paper.
    key: (optional) Key is a mapping of elements to a comparable key, similar
      to the key argument of Python's sorting methods.
    reverse: (optional) whether to order things smallest to largest, rather
      than largest to smallest.
    weighted: (optional) if set to True, the combiner produces weighted
      quantiles. The input elements are then expected to be tuples of input
      values with the corresponding weight.
  """

  # For alternating between biasing up and down in the above even weight
  # collapse operation.
  _offset_jitter = 0

  # The cost (in time and space) to compute quantiles to a given accuracy is a
  # function of the total number of elements in the data set. If an estimate is
  # not known or specified, we use this as an upper bound. If this is too low,
  # errors may exceed the requested tolerance; if too high, efficiency may be
  # non-optimal. The impact is logarithmic with respect to this value, so this
  # default should be fine for most uses.
  _MAX_NUM_ELEMENTS = 1e9
  _qs = None  # type: _QuantileState[T]

  def __init__(
      self,
      num_quantiles,  # type: int
      buffer_size,  # type: int
      num_buffers,  # type: int
      key=None,
      reverse=False,
      weighted=False):
    def _comparator(a, b):
      if key:
        a, b = key(a), key(b)

      retval = int(a > b) - int(a < b)

      if reverse:
        return -retval

      return retval

    self._comparator = _comparator

    self._num_quantiles = num_quantiles
    self._buffer_size = buffer_size
    self._num_buffers = num_buffers
    if weighted:
      self._key = (lambda x: x[0]) if key is None else (lambda x: key(x[0]))
    else:
      self._key = key
    self._reverse = reverse
    self._weighted = weighted

  @classmethod
  def create(
      cls,
      num_quantiles,  # type: int
      epsilon=None,
      max_num_elements=None,
      key=None,
      reverse=False,
      weighted=False):
    # type: (...) -> ApproximateQuantilesCombineFn

    """
    Creates an approximate quantiles combiner with the given key and desired
    number of quantiles.

    Args:
      num_quantiles: Number of quantiles to produce. It is the size of the
        final output list, including the mininum and maximum value items.
      epsilon: (optional) The default error bound is `epsilon`, which holds as
        long as the number of elements is less than `_MAX_NUM_ELEMENTS`.
        Specifically, if one considers the input as a sorted list x_1, ...,
        x_N, then the distance between each exact quantile x_c and its
        approximation x_c' is bounded by `|c - c'| < epsilon * N`. Note that
        these errors are worst-case scenarios. In practice the accuracy tends
        to be much better.
      max_num_elements: (optional) The cost (in time and space) to compute
        quantiles to a given accuracy is a function of the total number of
        elements in the data set.
      key: (optional) Key is a mapping of elements to a comparable key, similar
        to the key argument of Python's sorting methods.
      reverse: (optional) whether to order things smallest to largest, rather
        than largest to smallest.
      weighted: (optional) if set to True, the combiner produces weighted
        quantiles. The input elements are then expected to be tuples of values
        with the corresponding weight.
    """
    max_num_elements = max_num_elements or cls._MAX_NUM_ELEMENTS
    if not epsilon:
      epsilon = min(1e-2, 1.0 / num_quantiles) \
        if weighted else (1.0 / num_quantiles)
    b = 2
    while (b - 2) * (1 << (b - 2)) < epsilon * max_num_elements:
      b = b + 1
    b = b - 1
    k = max(2, int(math.ceil(max_num_elements / float(1 << (b - 1)))))
    return cls(
        num_quantiles=num_quantiles,
        buffer_size=k,
        num_buffers=b,
        key=key,
        reverse=reverse,
        weighted=weighted)

  def _add_unbuffered(self, qs, elements):
    # type: (_QuantileState[T], Iterable[T]) -> None

    """
    Add a new buffer to the unbuffered list, creating a new buffer and
    collapsing if needed.
    """
    qs.unbuffered_elements.extend(elements)
    if len(qs.unbuffered_elements) >= qs.buffer_size:
      qs.unbuffered_elements.sort(key=self._key, reverse=self._reverse)

      while len(qs.unbuffered_elements) >= qs.buffer_size:
        to_buffer = qs.unbuffered_elements[:qs.buffer_size]
        heapq.heappush(
            qs.buffers,
            _QuantileBuffer(
                elements=to_buffer,
                weighted=self._weighted,
                weight=sum([element[1] for element in to_buffer])
                if self._weighted else 1))
        qs.unbuffered_elements = qs.unbuffered_elements[qs.buffer_size:]
        self._collapse_if_needed(qs)

  def _offset(self, new_weight):
    # type: (int) -> float

    """
    If the weight is even, we must round up or down. Alternate between these
    two options to avoid a bias.
    """
    if new_weight % 2 == 1:
      return (new_weight + 1) / 2
    else:
      self._offset_jitter = 2 - self._offset_jitter
      return (new_weight + self._offset_jitter) / 2

  def _collapse(self, buffers):
    # type: (Iterable[_QuantileBuffer[T]]) -> _QuantileBuffer[T]
    new_level = 0
    new_weight = 0
    for buffer_elem in buffers:
      # As presented in the paper, there should always be at least two
      # buffers of the same (minimal) level to collapse, but it is possible
      # to violate this condition when combining buffers from independently
      # computed shards.  If they differ we take the max.
      new_level = max([new_level, buffer_elem.level + 1])
      new_weight = new_weight + buffer_elem.weight
    if self._weighted:
      step = new_weight / (self._buffer_size - 1)
      offset = new_weight / (2 * self._buffer_size)
    else:
      step = new_weight
      offset = self._offset(new_weight)
    new_elements = self._interpolate(buffers, self._buffer_size, step, offset)
    return _QuantileBuffer(new_elements, self._weighted, new_level, new_weight)

  def _collapse_if_needed(self, qs):
    # type: (_QuantileState) -> None
    while len(qs.buffers) > self._num_buffers:
      to_collapse = []
      to_collapse.append(heapq.heappop(qs.buffers))
      to_collapse.append(heapq.heappop(qs.buffers))
      min_level = to_collapse[1].level

      while len(qs.buffers) > 0 and qs.buffers[0].level == min_level:
        to_collapse.append(heapq.heappop(qs.buffers))

      heapq.heappush(qs.buffers, self._collapse(to_collapse))

  def _interpolate(self, i_buffers, count, step, offset):
    """
    Emulates taking the ordered union of all elements in buffers, repeated
    according to their weight, and picking out the (k * step + offset)-th
    elements of this list for `0 <= k < count`.
    """

    iterators = []
    new_elements = []
    compare_key = self._key
    if self._key and not self._weighted:
      compare_key = lambda x: self._key(x[0])
    for buffer_elem in i_buffers:
      iterators.append(buffer_elem.sized_iterator())

    # Python 3 `heapq.merge` support key comparison and returns an iterator and
    # does not pull the data into memory all at once. Python 2 does not
    # support comparison on its `heapq.merge` api, so we use the itertools
    # which takes the `key` function for comparison and creates an iterator
    # from it.
    if sys.version_info[0] < 3:
      sorted_elem = iter(
          sorted(
              itertools.chain.from_iterable(iterators),
              key=compare_key,
              reverse=self._reverse))
    else:
      sorted_elem = heapq.merge(
          *iterators, key=compare_key, reverse=self._reverse)

    weighted_element = next(sorted_elem)
    current = weighted_element[1]
    j = 0
    previous = 0
    while j < count:
      target = j * step + offset
      j = j + 1
      try:
        while current <= target:
          weighted_element = next(sorted_elem)
          current = current + weighted_element[1]
      except StopIteration:
        pass
      if self._weighted:
        new_elements.append((weighted_element[0], current - previous))
        previous = current
      else:
        new_elements.append(weighted_element[0])
    return new_elements

  # TODO(BEAM-7746): Signature incompatible with supertype
  def create_accumulator(self):  # type: ignore[override]
    # type: () -> _QuantileState[T]
    self._qs = _QuantileState(
        buffer_size=self._buffer_size,
        num_buffers=self._num_buffers,
        unbuffered_elements=[],
        buffers=[])
    return self._qs

  def add_input(self, quantile_state, element):
    """
    Add a new element to the collection being summarized by quantile state.
    """
    value = element[0] if self._weighted else element
    if quantile_state.is_empty():
      quantile_state.min_val = quantile_state.max_val = value
    elif self._comparator(value, quantile_state.min_val) < 0:
      quantile_state.min_val = value
    elif self._comparator(value, quantile_state.max_val) > 0:
      quantile_state.max_val = value
    self._add_unbuffered(quantile_state, elements=[element])
    return quantile_state

  def add_inputs(self, quantile_state, elements):
    """Add new elements to the collection being summarized by quantile state.
    """
    if not elements:
      return quantile_state

    values = [
        element[0] for element in elements
    ] if self._weighted else elements
    min_val = min(values)
    max_val = max(values)
    if quantile_state.is_empty():
      quantile_state.min_val = min_val
      quantile_state.max_val = max_val
    elif self._comparator(min_val, quantile_state.min_val) < 0:
      quantile_state.min_val = min_val
    elif self._comparator(max_val, quantile_state.max_val) > 0:
      quantile_state.max_val = max_val
    self._add_unbuffered(quantile_state, elements=elements)
    return quantile_state

  def merge_accumulators(self, accumulators):
    """Merges all the accumulators (quantile state) as one."""
    qs = self.create_accumulator()
    for accumulator in accumulators:
      if accumulator.is_empty():
        continue
      if not qs.min_val or self._comparator(accumulator.min_val,
                                            qs.min_val) < 0:
        qs.min_val = accumulator.min_val
      if not qs.max_val or self._comparator(accumulator.max_val,
                                            qs.max_val) > 0:
        qs.max_val = accumulator.max_val

      self._add_unbuffered(qs, accumulator.unbuffered_elements)

      qs.buffers.extend(accumulator.buffers)
    self._collapse_if_needed(qs)
    return qs

  def extract_output(self, accumulator):
    """
    Outputs num_quantiles elements consisting of the minimum, maximum and
    num_quantiles - 2 evenly spaced intermediate elements. Returns the empty
    list if no elements have been added.
    """
    if accumulator.is_empty():
      return []

    all_elems = accumulator.buffers
    if self._weighted:
      unbuffered_weight = sum(
          [element[1] for element in accumulator.unbuffered_elements])
      total_weight = unbuffered_weight
      for buffer_elem in all_elems:
        total_weight += sum([element[1] for element in buffer_elem.elements])
      if accumulator.unbuffered_elements:
        accumulator.unbuffered_elements.sort(
            key=self._key, reverse=self._reverse)
        all_elems.append(
            _QuantileBuffer(
                accumulator.unbuffered_elements,
                weighted=True,
                weight=unbuffered_weight))

      step = 1.0 * total_weight / (self._num_quantiles - 1)
      offset = (1.0 * total_weight) / (self._num_quantiles - 1)
      mid_quantiles = [
          element[0] for element in self._interpolate(
              all_elems, self._num_quantiles - 2, step, offset)
      ]
    else:
      total_weight = len(accumulator.unbuffered_elements)
      for buffer_elem in all_elems:
        total_weight += accumulator.buffer_size * buffer_elem.weight

      if accumulator.unbuffered_elements:
        accumulator.unbuffered_elements.sort(
            key=self._key, reverse=self._reverse)
        all_elems.append(
            _QuantileBuffer(accumulator.unbuffered_elements, weighted=False))

      step = 1.0 * total_weight / (self._num_quantiles - 1)
      offset = (1.0 * total_weight - 1) / (self._num_quantiles - 1)
      mid_quantiles = self._interpolate(
          all_elems, self._num_quantiles - 2, step, offset)

    quantiles = [accumulator.min_val]
    quantiles.extend(mid_quantiles)
    quantiles.append(accumulator.max_val)
    return quantiles
