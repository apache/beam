# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cimport cython
cimport libc.stdint

cdef class OperationCounters(object):
  cdef public _counter_factory
  cdef public element_counter
  cdef public mean_byte_counter
  cdef public coder
  cdef public _active_accumulators
  cdef public libc.stdint.int64_t _sample_counter
  cdef public libc.stdint.int64_t _next_sample

  cpdef update_from(self, windowed_value, coder=*)
  cpdef update_collect(self)

  cdef libc.stdint.int64_t _compute_next_sample(self, libc.stdint.int64_t i)
  cdef bint should_sample(self)
