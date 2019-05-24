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

"""This module has all statistic related transforms."""

from __future__ import absolute_import
from __future__ import division

import heapq
import math
import sys
from builtins import round

import mmh3

from apache_beam import coders
from apache_beam import typehints
from apache_beam.transforms.core import *
from apache_beam.transforms.ptransform import PTransform

__all__ = [
    'ApproximateUnique',
]

# Type variables
T = typehints.TypeVariable('T')
K = typehints.TypeVariable('K')
V = typehints.TypeVariable('V')


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

  @typehints.with_input_types(typehints.KV[K, V])
  @typehints.with_output_types(typehints.KV[K, int])
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
  _HASH_SPACE_SIZE = 2.0 * sys.maxsize

  def __init__(self, sample_size):
    self._sample_size = sample_size
    self._min_hash = sys.maxsize
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
      sample_space_size = sys.maxsize - 1.0 * self._min_hash
      est = (math.log1p(-self._sample_size / sample_space_size)
             / math.log1p(-1 / sample_space_size)
             * self._HASH_SPACE_SIZE
             / sample_space_size)

      return round(est)


class ApproximateUniqueCombineFn(CombineFn):
  """
  ApproximateUniqueCombineFn computes an estimate of the number of
  unique values that were combined.
  """

  def __init__(self, sample_size, coder):
    self._sample_size = sample_size
    self._coder = coder

  def create_accumulator(self, *args, **kwargs):
    return _LargestUnique(self._sample_size)

  def add_input(self, accumulator, element, *args, **kwargs):
    try:
      accumulator.add(mmh3.hash64(self._coder.encode(element))[1])
      return accumulator
    except Exception as e:
      raise RuntimeError("Runtime exception: %s", e)

  # created an issue https://issues.apache.org/jira/browse/BEAM-7285 to speep up
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
