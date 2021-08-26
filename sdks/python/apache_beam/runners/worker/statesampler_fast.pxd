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

from apache_beam.metrics.execution cimport MetricsContainer

from cpython cimport pythread
from libc.stdint cimport int32_t, int64_t

cdef class StateSampler(object):
  """Tracks time spent in states during pipeline execution."""
  cdef int _sampling_period_ms
  cdef int _sampling_period_ms_start
  cdef double _sampling_period_ratio

  cdef list scoped_states_by_index

  cdef public bint started
  cdef public bint finished
  cdef object sampling_thread

  # This lock guards members that are shared between threads, specificaly
  # finished, scoped_states_by_index, and the nsecs field of each state therein.
  cdef pythread.PyThread_type_lock lock

  cdef public int64_t state_transition_count
  cdef public int64_t time_since_transition

  cdef int32_t current_state_index

  cpdef ScopedState current_state(self)
  cdef inline ScopedState current_state_c(self)

  cpdef _scoped_state(
      self, counter_name, name_context, output_counter, metrics_container)

cdef class ScopedState(object):
  """Context manager class managing transitions for a given sampler state."""

  cdef readonly StateSampler sampler
  cdef readonly int32_t state_index
  cdef readonly object counter
  cdef readonly object name
  cdef readonly object name_context
  cdef readonly int64_t _nsecs
  cdef int32_t old_state_index
  cdef readonly MetricsContainer metrics_container

  cpdef __enter__(self)

  cpdef __exit__(self, unused_exc_type, unused_exc_value, unused_traceback)
