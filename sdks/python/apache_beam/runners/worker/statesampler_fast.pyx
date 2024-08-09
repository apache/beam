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

"""State sampler for tracking time spent in execution steps.

The state sampler profiles the time spent in each step of a pipeline.
Operations (defined in executor.py) which are executed as part of a MapTask are
instrumented with context managers provided by StateSampler.scoped_state().
These context managers change the internal state of the StateSampler during each
relevant Operation's .start(), .process() and .finish() methods.  State is
sampled by a raw C thread, not holding the Python Global Interpreter Lock, which
queries the StateSampler's internal state at a defined sampling frequency.  In a
common example, a ReadOperation during its .start() method reads an element and
calls a DoOperation's .process() method, which can call a WriteOperation's
.process() method.  Each element processed causes the current state to
transition between these states of different Operations.  Each time the sampling
thread queries the current state, the time spent since the previous sample is
attributed to that state and accumulated.  Over time, this allows a granular
runtime profile to be produced.
"""
import threading

from apache_beam.utils.counters import CounterName
from apache_beam.metrics.execution cimport MetricsContainer

cimport cython
from cpython cimport pythread
from libc cimport math
from libc.stdint cimport int32_t, int64_t


cdef extern from "Python.h":
  # This typically requires the GIL, but we synchronize the list modifications
  # we use this on via our own lock.
  cdef void* PyList_GET_ITEM(list, Py_ssize_t index) nogil

cdef extern from "crossplatform_unistd.h" nogil:
  void usleep(int)

cdef extern from "crossplatform_time.h" nogil:
  struct timespec:
    long tv_sec  # seconds
    long tv_nsec  # nanoseconds
  int clock_gettime(int clock_id, timespec *result)

cdef inline int64_t get_nsec_time() noexcept nogil:
  """Get current time as microseconds since Unix epoch."""
  cdef timespec current_time
  # First argument value of 0 corresponds to CLOCK_REALTIME.
  clock_gettime(0, &current_time)
  return (
      (<int64_t> current_time.tv_sec) * 1000000000 +  # second to nanoseconds
      current_time.tv_nsec)


cdef class StateSampler(object):
  """Tracks time spent in states during pipeline execution."""

  def __init__(self,
               sampling_period_ms,
               sampling_period_ms_start=None,
               sampling_period_ratio=1.2):
    self._sampling_period_ms = sampling_period_ms
    # Slowly ramp up to avoid excessive waiting for short stages, as well
    # as more precise information in that case.
    self._sampling_period_ms_start = (
          sampling_period_ms_start or max(1, sampling_period_ms // 100))
    self._sampling_period_ratio = sampling_period_ratio
    self.started = False
    self.finished = False

    self.lock = pythread.PyThread_allocate_lock()

    self.current_state_index = 0
    self.time_since_transition = 0
    self.state_transition_count = 0
    unknown_state = ScopedState(self,
                                CounterName('unknown'),
                                None,
                                self.current_state_index,
                                None,
                                None)
    pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
    self.scoped_states_by_index = [unknown_state]
    pythread.PyThread_release_lock(self.lock)

    # Assert that the compiler correctly aligned the current_state field.  This
    # is necessary for reads and writes to this variable to be atomic across
    # threads without additional synchronization.
    # States are referenced via an index rather than, say, a pointer because
    # of better support for 32-bit atomic reads and writes.
    assert (<int64_t> &self.current_state_index) % sizeof(int32_t) == 0, (
        'Address of StateSampler.current_state_index is not word-aligned.')

  def __dealloc__(self):
    pythread.PyThread_free_lock(self.lock)

  def run(self):
    cdef int64_t last_nsecs = get_nsec_time()
    cdef int64_t elapsed_nsecs
    cdef int64_t latest_transition_count = self.state_transition_count
    cdef int64_t sampling_period_us = self._sampling_period_ms_start * 1000
    with nogil:
      while True:
        usleep(<int>sampling_period_us)
        sampling_period_us = <int64_t>math.fmin(
            sampling_period_us * self._sampling_period_ratio,
            self._sampling_period_ms * 1000)
        pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
        try:
          if self.finished:
            break
          elapsed_nsecs = get_nsec_time() - last_nsecs
          # Take an address as we can't create a reference to the scope
          # without the GIL.
          nsecs_ptr = &(<ScopedState>PyList_GET_ITEM(
              self.scoped_states_by_index, self.current_state_index))._nsecs
          nsecs_ptr[0] += elapsed_nsecs
          if latest_transition_count != self.state_transition_count:
            self.time_since_transition = 0
            latest_transition_count = self.state_transition_count
          self.time_since_transition += elapsed_nsecs
          last_nsecs += elapsed_nsecs
        finally:
          pythread.PyThread_release_lock(self.lock)

  def start(self):
    assert not self.started
    self.sampling_thread = threading.Thread(target=self.run)
    self.sampling_thread.start()

  def stop(self):
    assert not self.finished
    pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
    self.finished = True
    pythread.PyThread_release_lock(self.lock)
    # May have to wait up to sampling_period_ms, but the platform-independent
    # pythread doesn't support conditions.
    self.sampling_thread.join()

  def reset(self):
    for state in self.scoped_states_by_index:
      (<ScopedState>state)._nsecs = 0
    self.started = self.finished = False

  cpdef ScopedState current_state(self):
    return self.current_state_c()

  cdef inline ScopedState current_state_c(self):
    # Faster than cpdef due to self always being a Python subclass.
    return <ScopedState>self.scoped_states_by_index[self.current_state_index]

  cpdef _scoped_state(self, counter_name, name_context, output_counter,
                      metrics_container):
    """Returns a context manager managing transitions for a given state.
    Args:
     counter_name: A CounterName object with information about the execution
       state.
     output_counter: A Beam Counter to which msecs are committed for reporting.
     metrics_container: A MetricsContainer for the current step.

    Returns:
      A ScopedState for the set of step-state-io_target.
    """
    new_state_index = len(self.scoped_states_by_index)
    scoped_state = ScopedState(self,
                               counter_name,
                               name_context,
                               new_state_index,
                               output_counter,
                               metrics_container)
    # Both scoped_states_by_index and scoped_state.nsecs are accessed
    # by the sampling thread; initialize them under the lock.
    pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
    self.scoped_states_by_index.append(scoped_state)
    scoped_state._nsecs = 0
    pythread.PyThread_release_lock(self.lock)
    return scoped_state

  def update_metric(self, typed_metric_name, value):
    # Each of these is a cdef lookup.
    metrics_container = self.current_state_c().metrics_container
    if metrics_container is not None:
      metrics_container.get_metric_cell(typed_metric_name).update(value)


cdef class ScopedState(object):
  """Context manager class managing transitions for a given sampler state."""

  def __init__(self,
               sampler,
               name,
               step_name_context,
               state_index,
               counter,
               metrics_container):
    self.sampler = sampler
    self.name = name
    self.name_context = step_name_context
    self.state_index = state_index
    self.counter = counter
    self.metrics_container = metrics_container

  @property
  def nsecs(self):
    return self._nsecs

  def sampled_seconds(self):
    return 1e-9 * self.nsecs

  def sampled_msecs_int(self):
    return int(1e-6 * self.nsecs)

  def __repr__(self):
    return "ScopedState[%s, %s]" % (self.name, self.nsecs)

  cpdef __enter__(self):
    self.old_state_index = self.sampler.current_state_index
    pythread.PyThread_acquire_lock(self.sampler.lock, pythread.WAIT_LOCK)
    self.sampler.current_state_index = self.state_index
    self.sampler.state_transition_count += 1
    pythread.PyThread_release_lock(self.sampler.lock)

  cpdef __exit__(self, unused_exc_type, unused_exc_value, unused_traceback):
    pythread.PyThread_acquire_lock(self.sampler.lock, pythread.WAIT_LOCK)
    self.sampler.current_state_index = self.old_state_index
    self.sampler.state_transition_count += 1
    pythread.PyThread_release_lock(self.sampler.lock)
