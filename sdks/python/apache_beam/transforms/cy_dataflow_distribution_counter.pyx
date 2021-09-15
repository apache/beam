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

# cython: profile=True
# cython: language_level=3

""" For internal use only. No backwards compatibility guarantees."""

cimport cython
from libc.stdint cimport int64_t, INT64_MAX
from libc.stdlib cimport calloc, free


cdef unsigned long long* POWER_TEN = [10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5,
                                      10e6, 10e7, 10e8, 10e9, 10e10, 10e11,
                                      10e12, 10e13, 10e14, 10e15, 10e16, 10e17,
                                      10e18]


cdef int64_t get_log10_round_to_floor(int64_t element):
  cdef int power = 0
  while element >= POWER_TEN[power]:
    power += 1
  return power - 1


cdef class DataflowDistributionCounter(object):
  """Distribution Counter:

  Contains value distribution statistics and methods for incrementing.

  Currently using special bucketing strategy suitable for Dataflow

  Attributes:
    min: minimum value of all inputs.
    max: maximum value of all inputs.
    count: total count of all inputs.
    sum: sum of all inputs.
    buckets: histogram buckets of value counts for a
    distribution(1,2,5 bucketing). Max bucket_index is 58( sys.maxint as input).
    is_cythonized: mark whether DataflowDistributionCounter cythonized.
  """
  def __init__(self):
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    self.buckets = <int64_t*> calloc(MAX_BUCKET_SIZE, sizeof(int64_t))
    self.is_cythonized = True

  def __dealloc__(self):
    """free allocated memory"""
    free(self.buckets)

  cpdef bint add_input(self, int64_t element) except -1:
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    cdef int64_t bucket_index = self._fast_calculate_bucket_index(element)
    self.buckets[bucket_index] += 1

  cdef int64_t _fast_calculate_bucket_index(self, int64_t element):
    """Calculate the bucket index for the given element.

    Declare calculate_bucket_index as cdef in order to improve performance,
    since cpdef will have significant overhead.
    """
    if element == 0:
      return 0
    cdef int64_t log10_floor = get_log10_round_to_floor(element)
    cdef int64_t power_of_ten = POWER_TEN[log10_floor]
    cdef int64_t bucket_offset = 0
    if element < power_of_ten * 2:
      bucket_offset = 0
    elif element < power_of_ten * 5:
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + log10_floor * BUCKET_PER_TEN + bucket_offset

  cpdef void translate_to_histogram(self, histogram):
    """Translate buckets into Histogram.

    Args:
      histogram: apache_beam.runners.dataflow.internal.clents.dataflow.Histogram
      Ideally, only call this function when reporting counter to
      dataflow service.
    """
    cdef int first_bucket_offset = 0
    cdef int last_bucket_offset = 0
    cdef int index = 0
    for index in range(0, MAX_BUCKET_SIZE):
      if self.buckets[index] != 0:
        first_bucket_offset = index
        break
    for index in range(MAX_BUCKET_SIZE - 1, -1, -1):
      if self.buckets[index] != 0:
        last_bucket_offset = index
        break
    histogram.firstBucketOffset = first_bucket_offset
    histogram.bucketCounts = []
    for index in range(first_bucket_offset, last_bucket_offset + 1):
      histogram.bucketCounts.append(self.buckets[index])

  cpdef bint add_inputs_for_test(self, elements) except -1:
    """Used for performance microbenchmark.

    During runtime, add_input will be called through c-call, so we want to have
    the same calling routine when running microbenchmark as application runtime.
    Directly calling cpdef from def will cause significant overhead.
    """
    for element in elements:
      self.add_input(element)

  cpdef int64_t calculate_bucket_index(self, int64_t element):
    """Used for unit tests.

    cdef calculate_bucket_index cannot be called directly from def.
    """
    return self._fast_calculate_bucket_index(element)

  cpdef merge(self, accumulators):
    raise NotImplementedError()
