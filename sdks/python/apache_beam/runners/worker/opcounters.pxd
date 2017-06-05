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
cimport libc.stdint

from apache_beam.utils.counters cimport Counter


cdef class SumAccumulator(object):
  cdef libc.stdint.int64_t _value
  cpdef update(self, libc.stdint.int64_t value)
  cpdef libc.stdint.int64_t value(self)


cdef class OperationCounters(object):
  cdef public _counter_factory
  cdef public Counter element_counter
  cdef public Counter mean_byte_counter
  cdef public coder_impl
  cdef public SumAccumulator active_accumulator
  cdef public libc.stdint.int64_t _sample_counter
  cdef public libc.stdint.int64_t _next_sample

  cpdef update_from(self, windowed_value)
  cdef inline do_sample(self, windowed_value)
  cpdef update_collect(self)

  cdef libc.stdint.int64_t _compute_next_sample(self, libc.stdint.int64_t i)
  cdef inline bint _should_sample(self)
  cpdef bint should_sample(self)
