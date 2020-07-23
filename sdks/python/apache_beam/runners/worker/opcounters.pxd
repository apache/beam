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
from apache_beam.runners.worker cimport statesampler_fast


cdef class TransformIOCounter(object):
  cdef readonly object _counter_factory
  cdef readonly object _state_sampler
  cdef Counter bytes_read_counter
  cdef statesampler_fast.ScopedState scoped_state
  cdef object _latest_step

  cpdef update_current_step(self)
  cpdef add_bytes_read(self, libc.stdint.int64_t n)
  cpdef __enter__(self)
  cpdef __exit__(self, exc_type, exc_value, traceback)


cdef class NoOpTransformIOCounter(TransformIOCounter):
  pass


cdef class SideInputReadCounter(TransformIOCounter):
  cdef readonly object declaring_step
  cdef readonly object input_index


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
  cdef public object current_size
  cdef public libc.stdint.int64_t _sample_counter
  cdef public libc.stdint.int64_t _next_sample

  cpdef update_from(self, windowed_value)
  cdef inline do_sample(self, windowed_value)
  cpdef update_collect(self)
  cpdef type_check(self, value, is_input)

  cdef libc.stdint.int64_t _compute_next_sample(self, libc.stdint.int64_t i)
  cdef inline bint _should_sample(self)
  cpdef bint should_sample(self)
