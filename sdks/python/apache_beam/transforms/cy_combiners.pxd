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

cimport cython
from libc.stdint cimport int64_t, INT64_MIN, INT64_MAX

cdef double _NEG_INF, _POS_INF, _NAN


cdef class CountAccumulator(object):
  cdef readonly int64_t value
  cpdef add_input(self, unused_element)
  @cython.locals(accumulator=CountAccumulator)
  cpdef merge(self, accumulators)

cdef class SumInt64Accumulator(object):
  cdef readonly int64_t value
  cpdef add_input(self, int64_t element)
  @cython.locals(accumulator=SumInt64Accumulator)
  cpdef merge(self, accumulators)

cdef class MinInt64Accumulator(object):
  cdef readonly int64_t value
  cpdef add_input(self, int64_t element)
  @cython.locals(accumulator=MinInt64Accumulator)
  cpdef merge(self, accumulators)

cdef class MaxInt64Accumulator(object):
  cdef readonly int64_t value
  cpdef add_input(self, int64_t element)
  @cython.locals(accumulator=MaxInt64Accumulator)
  cpdef merge(self, accumulators)

cdef class MeanInt64Accumulator(object):
  cdef readonly int64_t sum
  cdef readonly int64_t count
  cpdef add_input(self, int64_t element)
  @cython.locals(accumulator=MeanInt64Accumulator)
  cpdef merge(self, accumulators)


cdef class SumDoubleAccumulator(object):
  cdef readonly double value
  cpdef add_input(self, double element)
  @cython.locals(accumulator=SumDoubleAccumulator)
  cpdef merge(self, accumulators)

cdef class MinDoubleAccumulator(object):
  cdef readonly double value
  cpdef add_input(self, double element)
  @cython.locals(accumulator=MinDoubleAccumulator)
  cpdef merge(self, accumulators)

cdef class MaxDoubleAccumulator(object):
  cdef readonly double value
  cpdef add_input(self, double element)
  @cython.locals(accumulator=MaxDoubleAccumulator)
  cpdef merge(self, accumulators)

cdef class MeanDoubleAccumulator(object):
  cdef readonly double sum
  cdef readonly int64_t count
  cpdef add_input(self, double element)
  @cython.locals(accumulator=MeanDoubleAccumulator)
  cpdef merge(self, accumulators)


cdef class AllAccumulator(object):
  cdef readonly bint value
  cpdef add_input(self, bint element)
  @cython.locals(accumulator=AllAccumulator)
  cpdef merge(self, accumulators)

cdef class AnyAccumulator(object):
  cdef readonly bint value
  cpdef add_input(self, bint element)
  @cython.locals(accumulator=AnyAccumulator)
  cpdef merge(self, accumulators)

cdef bint compare_to(int64_t x, int64_t y)

@cython.locals(number_of_leading_zeros=int64_t, y=int64_t)
cdef int64_t get_log10_round_to_floor(int64_t element)

cdef class DistributionAccumulator(object):
  cdef public int64_t min
  cdef public int64_t max
  cdef public int64_t count
  cdef public int64_t sum
  cdef public int64_t first_bucket_offset
  cdef public list buckets
  cdef public int64_t buckets_per_10
  @cython.locals(bucket_index = int64_t, size_of_bucket=int64_t)
  cpdef add_input(self, int64_t element)
  @cython.locals(log10_floor=int64_t, power_of_ten=int64_t,
                 bucket_offset=int64_t)
  cpdef int64_t calculate_bucket_index(self, int64_t element)
  cdef void increment_bucket(self, int64_t bucket_index)
