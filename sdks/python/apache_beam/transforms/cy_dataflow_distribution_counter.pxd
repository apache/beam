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

""" For internal use only. No backwards compatibility guarantees."""

cimport cython
from libc.stdint cimport int64_t


# 3 buckets for every power of ten -> 1, 2, 5
cdef enum:
  BUCKET_PER_TEN = 3

# Assume the max input is max(int64_t), then the possible max bucket size is 59
cdef enum:
  MAX_BUCKET_SIZE = 59

cdef class DataflowDistributionCounter(object):
  cdef public int64_t min
  cdef public int64_t max
  cdef public int64_t count
  cdef public int64_t sum
  cdef int64_t* buckets
  cdef public bint is_cythonized
  cpdef bint add_input(self, int64_t element) except -1
  cdef int64_t _fast_calculate_bucket_index(self, int64_t element)
  cpdef void translate_to_histogram(self, histogram)
  cpdef bint add_inputs_for_test(self, elements) except -1
  cpdef int64_t calculate_bucket_index(self, int64_t element)
  cpdef merge(self, accumulators)
