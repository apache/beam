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

import hashlib
import heapq
import itertools
import logging
import math
import typing
from collections.abc import Callable
from typing import Any
from typing import List
from typing import Tuple

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

try:
  import mmh3  # pylint: disable=import-error

  def _mmh3_hash(value):
    # mmh3.hash64 returns two 64-bit unsigned integers
    return mmh3.hash64(value, seed=0, signed=False)[0]

  _default_hash_fn = _mmh3_hash
  _default_hash_fn_type = 'mmh3'
except ImportError:

  def _md5_hash(value):
    # md5 is a 128-bit hash, so we truncate the hexdigest (string of 32
    # hexadecimal digits) to 16 digits and convert to int to get the 64-bit
    # integer fingerprint.
    return int(hashlib.md5(value).hexdigest()[:16], 16)

  _default_hash_fn = _md5_hash
  _default_hash_fn_type = 'md5'


def _get_default_hash_fn():
  """Returns either murmurhash or md5 based on installation."""
  if _default_hash_fn_type == 'md5':
    logging.warning(
        'Couldn\'t find murmurhash. Install mmh3 for a faster implementation of'
        'ApproximateUnique.')
  return _default_hash_fn


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
    return math.ceil(4.0 / math.pow(est_err, 2.0))

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

  # created an issue https://github.com/apache/beam/issues/19459 to speed up
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

    in: [list(range(10)), ..., list(range(90, 101))], num_quantiles=5,
      input_batched=True

    out: [0, 25, 50, 75, 100]

    in: [(list(range(10)), [1]*10), (list(range(10)), [0]*10), ...,
      (list(range(90, 101)), [0]*11)], num_quantiles=5, input_batched=True,
      weighted=True

    out: [0, 2, 5, 7, 100]
  """
  @staticmethod
  def _display_data(num_quantiles, key, reverse, weighted, input_batched):
    return {
        'num_quantiles': DisplayDataItem(num_quantiles, label='Quantile Count'),
        'key': DisplayDataItem(
            key.__name__
            if hasattr(key, '__name__') else key.__class__.__name__,
            label='Record Comparer Key'),
        'reverse': DisplayDataItem(str(reverse), label='Is Reversed'),
        'weighted': DisplayDataItem(str(weighted), label='Is Weighted'),
        'input_batched': DisplayDataItem(
            str(input_batched), label='Is Input Batched'),
    }

  @typehints.with_input_types(
      typing.Union[T, typing.Sequence[T], Tuple[T, float]])
  @typehints.with_output_types(List[T])
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
      input_batched: (optional) if set to True, the transform expects each
        element of input PCollection to be a batch, which is a list of elements
        for non-weighted case and a tuple of lists of elements and weights for
        weighted. Provides a way to accumulate multiple elements at a time more
        efficiently.
    """
    def __init__(
        self,
        num_quantiles,
        key=None,
        reverse=False,
        weighted=False,
        input_batched=False):
      self._num_quantiles = num_quantiles
      self._key = key
      self._reverse = reverse
      self._weighted = weighted
      self._input_batched = input_batched

    def expand(self, pcoll):
      return pcoll | CombineGlobally(
          ApproximateQuantilesCombineFn.create(
              num_quantiles=self._num_quantiles,
              key=self._key,
              reverse=self._reverse,
              weighted=self._weighted,
              input_batched=self._input_batched))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self._num_quantiles,
          key=self._key,
          reverse=self._reverse,
          weighted=self._weighted,
          input_batched=self._input_batched)

  @typehints.with_input_types(
      typehints.Union[Tuple[K, V], Tuple[K, Tuple[V, float]]])
  @typehints.with_output_types(Tuple[K, List[V]])
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
      input_batched: (optional) if set to True, the transform expects each
        element of input PCollection to be a batch, which is a list of elements
        for non-weighted case and a tuple of lists of elements and weights for
        weighted. Provides a way to accumulate multiple elements at a time more
        efficiently.
    """
    def __init__(
        self,
        num_quantiles,
        key=None,
        reverse=False,
        weighted=False,
        input_batched=False):
      self._num_quantiles = num_quantiles
      self._key = key
      self._reverse = reverse
      self._weighted = weighted
      self._input_batched = input_batched

    def expand(self, pcoll):
      return pcoll | CombinePerKey(
          ApproximateQuantilesCombineFn.create(
              num_quantiles=self._num_quantiles,
              key=self._key,
              reverse=self._reverse,
              weighted=self._weighted,
              input_batched=self._input_batched))

    def display_data(self):
      return ApproximateQuantiles._display_data(
          num_quantiles=self._num_quantiles,
          key=self._key,
          reverse=self._reverse,
          weighted=self._weighted,
          input_batched=self._input_batched)


class _QuantileSpec(object):
  """Quantiles computation specifications."""
  def __init__(self, buffer_size, num_buffers, weighted, key, reverse):
    # type: (int, int, bool, Any, bool) -> None
    self.buffer_size = buffer_size
    self.num_buffers = num_buffers
    self.weighted = weighted
    self.key = key
    self.reverse = reverse

    # Used to sort tuples of values and weights.
    self.weighted_key = None if key is None else (lambda x: key(x[0]))

    # Used to compare values.
    if reverse and key is None:
      self.less_than = lambda a, b: a > b
    elif reverse:
      self.less_than = lambda a, b: key(a) > key(b)
    elif key is None:
      self.less_than = lambda a, b: a < b
    else:
      self.less_than = lambda a, b: key(a) < key(b)

  def get_argsort_key(self, elements):
    # type: (List) -> Callable[[int], Any]

    """Returns a key for sorting indices of elements by element's value."""
    if self.key is None:
      return elements.__getitem__
    else:
      return lambda idx: self.key(elements[idx])

  def __reduce__(self):
    return (
        self.__class__,
        (
            self.buffer_size,
            self.num_buffers,
            self.weighted,
            self.key,
            self.reverse))


class _QuantileBuffer(object):
  """A single buffer in the sense of the referenced algorithm.
  (see http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.6.6513&rep=rep1
  &type=pdf and ApproximateQuantilesCombineFn for further information)"""
  def __init__(
      self, elements, weights, weighted, level=0, min_val=None, max_val=None):
    # type: (list, list, bool, int, Any, Any) -> None
    self.elements = elements
    # In non-weighted case weights contains a single element representing weight
    # of the buffer in the sense of the original algorithm. In weighted case,
    # it stores weights of individual elements.
    self.weights = weights
    self.weighted = weighted
    self.level = level
    if min_val is None or max_val is None:
      # Buffer is always initialized with sorted elements.
      self.min_val = elements[0]
      self.max_val = elements[-1]
    else:
      # Note that collapsed buffer may not contain min and max in the list of
      # elements.
      self.min_val = min_val
      self.max_val = max_val

  def __iter__(self):
    return zip(
        self.elements,
        self.weights if self.weighted else itertools.repeat(self.weights[0]))

  def __lt__(self, other):
    return self.level < other.level


class _QuantileState(object):
  """
  Compact summarization of a collection on which quantiles can be estimated.
  """
  def __init__(self, unbuffered_elements, unbuffered_weights, buffers, spec):
    # type: (List, List, List[_QuantileBuffer], _QuantileSpec) -> None
    self.buffers = buffers
    self.spec = spec
    if spec.weighted:
      self.add_unbuffered = self._add_unbuffered_weighted
    else:
      self.add_unbuffered = self._add_unbuffered

    # The algorithm requires that the manipulated buffers always be filled to
    # capacity to perform the collapse operation. This operation can be extended
    # to buffers of varying sizes by introducing the notion of fractional
    # weights, but it's easier to simply combine the remainders from all shards
    # into new, full buffers and then take them into account when computing the
    # final output.
    self.unbuffered_elements = unbuffered_elements
    self.unbuffered_weights = unbuffered_weights

  # This is needed for pickling to work when Cythonization is enabled.
  def __reduce__(self):
    return (
        self.__class__,
        (
            self.unbuffered_elements,
            self.unbuffered_weights,
            self.buffers,
            self.spec))

  def is_empty(self):
    # type: () -> bool

    """Check if the buffered & unbuffered elements are empty or not."""
    return not self.unbuffered_elements and not self.buffers

  def _add_unbuffered(self, elements, offset_fn):
    # type: (List, Any) -> None

    """
    Add elements to the unbuffered list, creating new buffers and
    collapsing if needed.
    """
    self.unbuffered_elements.extend(elements)
    num_new_buffers = len(self.unbuffered_elements) // self.spec.buffer_size
    for idx in range(num_new_buffers):
      to_buffer = sorted(
          self.unbuffered_elements[idx * self.spec.buffer_size:(idx + 1) *
                                   self.spec.buffer_size],
          key=self.spec.key,
          reverse=self.spec.reverse)
      heapq.heappush(
          self.buffers,
          _QuantileBuffer(elements=to_buffer, weights=[1], weighted=False))

    if num_new_buffers > 0:
      self.unbuffered_elements = self.unbuffered_elements[num_new_buffers *
                                                          self.spec.
                                                          buffer_size:]

    self.collapse_if_needed(offset_fn)

  def _add_unbuffered_weighted(self, elements, offset_fn):
    # type: (List, Any) -> None

    """
    Add elements with weights to the unbuffered list, creating new buffers and
    collapsing if needed.
    """
    if len(elements) == 1:
      self.unbuffered_elements.append(elements[0][0])
      self.unbuffered_weights.append(elements[0][1])
    else:
      self.unbuffered_elements.extend(elements[0])
      self.unbuffered_weights.extend(elements[1])
    num_new_buffers = len(self.unbuffered_elements) // self.spec.buffer_size
    argsort_key = self.spec.get_argsort_key(self.unbuffered_elements)
    for idx in range(num_new_buffers):
      argsort = sorted(
          range(idx * self.spec.buffer_size, (idx + 1) * self.spec.buffer_size),
          key=argsort_key,
          reverse=self.spec.reverse)
      elements_to_buffer = [self.unbuffered_elements[idx] for idx in argsort]
      weights_to_buffer = [self.unbuffered_weights[idx] for idx in argsort]
      heapq.heappush(
          self.buffers,
          _QuantileBuffer(
              elements=elements_to_buffer,
              weights=weights_to_buffer,
              weighted=True))

    if num_new_buffers > 0:
      self.unbuffered_elements = self.unbuffered_elements[num_new_buffers *
                                                          self.spec.
                                                          buffer_size:]
      self.unbuffered_weights = self.unbuffered_weights[num_new_buffers *
                                                        self.spec.buffer_size:]

    self.collapse_if_needed(offset_fn)

  def finalize(self):
    # type: () -> None

    """
    Creates a new buffer using all unbuffered elements. Called before
    extracting an output. Note that the buffer doesn't have to be put in a
    proper position since _collapse is not going to be called after.
    """
    if self.unbuffered_elements and self.spec.weighted:
      argsort_key = self.spec.get_argsort_key(self.unbuffered_elements)
      argsort = sorted(
          range(len(self.unbuffered_elements)),
          key=argsort_key,
          reverse=self.spec.reverse)
      self.unbuffered_elements = [
          self.unbuffered_elements[idx] for idx in argsort
      ]
      self.unbuffered_weights = [
          self.unbuffered_weights[idx] for idx in argsort
      ]
      self.buffers.append(
          _QuantileBuffer(
              self.unbuffered_elements, self.unbuffered_weights, weighted=True))
      self.unbuffered_weights = []
    elif self.unbuffered_elements:
      self.unbuffered_elements.sort(
          key=self.spec.key, reverse=self.spec.reverse)
      self.buffers.append(
          _QuantileBuffer(
              self.unbuffered_elements, weights=[1], weighted=False))
    self.unbuffered_elements = []

  def collapse_if_needed(self, offset_fn):
    # type: (Any) -> None

    """
    Checks if summary has too many buffers and collapses some of them until the
    limit is restored.
    """
    while len(self.buffers) > self.spec.num_buffers:
      to_collapse = [heapq.heappop(self.buffers), heapq.heappop(self.buffers)]
      min_level = to_collapse[1].level

      while self.buffers and self.buffers[0].level <= min_level:
        to_collapse.append(heapq.heappop(self.buffers))

      heapq.heappush(self.buffers, _collapse(to_collapse, offset_fn, self.spec))


def _collapse(buffers, offset_fn, spec):
  # type: (List[_QuantileBuffer], Any, _QuantileSpec) -> _QuantileBuffer

  """
  Approximates elements from multiple buffers and produces a single buffer.
  """
  new_level = 0
  new_weight = 0
  for buffer in buffers:
    # As presented in the paper, there should always be at least two
    # buffers of the same (minimal) level to collapse, but it is possible
    # to violate this condition when combining buffers from independently
    # computed shards. If they differ we take the max.
    new_level = max([new_level, buffer.level + 1])
    new_weight = new_weight + sum(buffer.weights)
  if spec.weighted:
    step = new_weight / (spec.buffer_size - 1)
    offset = new_weight / (2 * spec.buffer_size)
  else:
    step = new_weight
    offset = offset_fn(new_weight)
  new_elements, new_weights, min_val, max_val = \
      _interpolate(buffers, spec.buffer_size, step, offset, spec)
  if not spec.weighted:
    new_weights = [new_weight]
  return _QuantileBuffer(
      new_elements, new_weights, spec.weighted, new_level, min_val, max_val)


def _interpolate(buffers, count, step, offset, spec):
  # type: (List[_QuantileBuffer], int, float, float, _QuantileSpec) -> Tuple[List, List, Any, Any]

  """
  Emulates taking the ordered union of all elements in buffers, repeated
  according to their weight, and picking out the (k * step + offset)-th elements
  of this list for `0 <= k < count`.
  """
  buffer_iterators = []
  min_val = buffers[0].min_val
  max_val = buffers[0].max_val
  for buffer in buffers:
    # Calculate extreme values for the union of buffers.
    min_val = buffer.min_val if spec.less_than(
        buffer.min_val, min_val) else min_val
    max_val = buffer.max_val if spec.less_than(
        max_val, buffer.max_val) else max_val
    buffer_iterators.append(iter(buffer))

  # Note that `heapq.merge` can also be used here since the buffers are sorted.
  # In practice, however, `sorted` uses natural order in the union and
  # significantly outperforms `heapq.merge`.
  sorted_elements = sorted(
      itertools.chain.from_iterable(buffer_iterators),
      key=spec.weighted_key,
      reverse=spec.reverse)

  if not spec.weighted:
    # If all buffers have the same weight, then quantiles' indices are evenly
    # distributed over a range [0, len(sorted_elements)].
    buffers_have_same_weight = True
    weight = buffers[0].weights[0]
    for buffer in buffers:
      if buffer.weights[0] != weight:
        buffers_have_same_weight = False
        break
    if buffers_have_same_weight:
      offset = offset / weight
      step = step / weight
      max_idx = len(sorted_elements) - 1
      result = [
          sorted_elements[min(int(j * step + offset), max_idx)][0]
          for j in range(count)
      ]
      return result, [], min_val, max_val

  sorted_elements_iter = iter(sorted_elements)
  weighted_element = next(sorted_elements_iter)
  new_elements = []
  new_weights = []
  j = 0
  current_weight = weighted_element[1]
  previous_weight = 0
  while j < count:
    target_weight = j * step + offset
    j += 1
    try:
      while current_weight <= target_weight:
        weighted_element = next(sorted_elements_iter)
        current_weight += weighted_element[1]
    except StopIteration:
      pass
    new_elements.append(weighted_element[0])
    if spec.weighted:
      new_weights.append(current_weight - previous_weight)
      previous_weight = current_weight

  return new_elements, new_weights, min_val, max_val


class ApproximateQuantilesCombineFn(CombineFn):
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

  Note that the weighted quantiles are evaluated using a generalized version of
  the algorithm referenced in the paper.

  The default error bound is (1 / num_quantiles) for uniformly distributed data
  and min(1e-2, 1 / num_quantiles) for weighted case, though in practice the
  accuracy tends to be much better.

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
    input_batched: (optional) if set to True, inputs are expected to be batches
      of elements.
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
  _qs = None  # type: _QuantileState

  def __init__(
      self,
      num_quantiles,  # type: int
      buffer_size,  # type: int
      num_buffers,  # type: int
      key=None,
      reverse=False,
      weighted=False,
      input_batched=False):
    self._num_quantiles = num_quantiles
    self._spec = _QuantileSpec(buffer_size, num_buffers, weighted, key, reverse)
    self._input_batched = input_batched
    if self._input_batched:
      setattr(self, 'add_input', self._add_inputs)

  def __reduce__(self):
    return (
        self.__class__,
        (
            self._num_quantiles,
            self._spec.buffer_size,
            self._spec.num_buffers,
            self._spec.key,
            self._spec.reverse,
            self._spec.weighted,
            self._input_batched))

  @classmethod
  def create(
      cls,
      num_quantiles,  # type: int
      epsilon=None,
      max_num_elements=None,
      key=None,
      reverse=False,
      weighted=False,
      input_batched=False):
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
      input_batched: (optional) if set to True, inputs are expected to be
        batches of elements.
    """
    max_num_elements = max_num_elements or cls._MAX_NUM_ELEMENTS
    if not epsilon:
      epsilon = min(1e-2, 1.0 / num_quantiles) \
        if weighted else (1.0 / num_quantiles)
    # Note that calculation of the buffer size and the number of buffers here
    # is based on technique used in the Munro-Paterson algorithm. Switching to
    # the logic used in the "New Algorithm" may result in memory savings since
    # it results in lower values for b and k in practice.
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
        weighted=weighted,
        input_batched=input_batched)

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

  # TODO(https://github.com/apache/beam/issues/19737): Signature incompatible
  # with supertype
  def create_accumulator(self):
    # type: () -> _QuantileState
    self._qs = _QuantileState(
        unbuffered_elements=[],
        unbuffered_weights=[],
        buffers=[],
        spec=self._spec)
    return self._qs

  def add_input(self, quantile_state, element):
    """
    Add a new element to the collection being summarized by quantile state.
    """
    quantile_state.add_unbuffered([element], self._offset)
    return quantile_state

  def _add_inputs(self, quantile_state, elements):
    # type: (_QuantileState, List) -> _QuantileState

    """
    Add a batch of elements to the collection being summarized by quantile
    state.
    """
    if len(elements) == 0:
      return quantile_state
    quantile_state.add_unbuffered(elements, self._offset)
    return quantile_state

  def merge_accumulators(self, accumulators):
    """Merges all the accumulators (quantile state) as one."""
    qs = self.create_accumulator()
    for accumulator in accumulators:
      if accumulator.is_empty():
        continue
      if self._spec.weighted:
        qs.add_unbuffered(
            [accumulator.unbuffered_elements, accumulator.unbuffered_weights],
            self._offset)
      else:
        qs.add_unbuffered(accumulator.unbuffered_elements, self._offset)

      qs.buffers.extend(accumulator.buffers)
    heapq.heapify(qs.buffers)
    qs.collapse_if_needed(self._offset)
    return qs

  def extract_output(self, accumulator):
    """
    Outputs num_quantiles elements consisting of the minimum, maximum and
    num_quantiles - 2 evenly spaced intermediate elements. Returns the empty
    list if no elements have been added.
    """
    if accumulator.is_empty():
      return []
    accumulator.finalize()
    all_elems = accumulator.buffers
    total_weight = 0
    if self._spec.weighted:
      for buffer_elem in all_elems:
        total_weight += sum(buffer_elem.weights)
    else:
      for buffer_elem in all_elems:
        total_weight += len(buffer_elem.elements) * buffer_elem.weights[0]

    step = total_weight / (self._num_quantiles - 1)
    offset = (total_weight - 1) / (self._num_quantiles - 1)

    quantiles, _, min_val, max_val = \
        _interpolate(all_elems, self._num_quantiles - 2, step, offset,
                     self._spec)

    return [min_val] + quantiles + [max_val]
