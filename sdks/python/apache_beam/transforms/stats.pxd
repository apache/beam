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
from libc.stdint cimport int64_t

cdef class _QuantileSpec(object):
  cdef readonly int64_t buffer_size
  cdef readonly int64_t num_buffers
  cdef readonly bint weighted
  cdef readonly key
  cdef readonly bint reverse
  cdef readonly weighted_key
  cdef readonly less_than

cdef class _QuantileBuffer(object):
  cdef readonly elements
  cdef readonly weights
  cdef readonly bint weighted
  cdef readonly int64_t level
  cdef readonly min_val
  cdef readonly max_val
  cdef readonly _iter

cdef class _QuantileState(object):
  cdef readonly _QuantileSpec spec
  cdef public buffers
  cdef public unbuffered_elements
  cdef public unbuffered_weights
  cdef public add_unbuffered
  cpdef bint is_empty(self)
  @cython.locals(num_new_buffers=int64_t, idx=int64_t)
  cpdef _add_unbuffered(self, elements, offset_fn)
  @cython.locals(num_new_buffers=int64_t, idx=int64_t)
  cpdef _add_unbuffered_weighted(self, elements, offset_fn)
  cpdef finalize(self)
  @cython.locals(min_level=int64_t)
  cpdef collapse_if_needed(self, offset_fn)


@cython.locals(new_level=int64_t, new_weight=double, step=double, offset=double)
cdef _QuantileBuffer _collapse(buffers, offset_fn, _QuantileSpec spec)

@cython.locals(j=int64_t)
cdef _interpolate(buffers, int64_t count, double step, double offset,
                  _QuantileSpec spec)