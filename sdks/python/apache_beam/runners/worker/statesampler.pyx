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

# cython: profile=True

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

Also measures time spent for the current active transition (A transition that
has entered but not exited a context). We keep track of the number of
completed transitions for every sample period and use it to attribute the
fraction of the time spent in the active transition for a sample:

  active_transition_time = elapsed_time * (1 / (1 + #completed transitions))

For shorter transitions (comparable to the sampling period), this may not be
accurate since there aren't enough samples, but provides a good estimate for
longer transitions.

The active transition time is mainly useful for progress reporting purposes.
"""

import threading
import time


from apache_beam.utils.counters import Counter


cimport cython
from cpython cimport pythread
from libc.stdint cimport int32_t, int64_t

cdef extern from "Python.h":
  # This typically requires the GIL, but we synchronize the list modifications
  # we use this on via our own lock.
  cdef void* PyList_GET_ITEM(list, Py_ssize_t index) nogil

cdef extern from "unistd.h" nogil:
  void usleep(long)

cdef extern from "<time.h>" nogil:
  struct timespec:
    long tv_sec  # seconds
    long tv_nsec  # nanoseconds
  int clock_gettime(int clock_id, timespec *result)

cdef inline int64_t get_nsec_time() nogil:
  """Get current time as microseconds since Unix epoch."""
  cdef timespec current_time
  # First argument value of 0 corresponds to CLOCK_REALTIME.
  clock_gettime(0, &current_time)
  return (
      (<int64_t> current_time.tv_sec) * 1000000000 +  # second to nanoseconds
      current_time.tv_nsec)


class StateSamplerInfo(object):
  """Info for current state and transition statistics of StateSampler."""

  def __init__(self, state_name, transition_count):
    self.state_name = state_name
    self.transition_count = transition_count

  def __repr__(self):
    return '<StateSamplerInfo %s %d>' % (self.state_name, self.transition_count)


# Default period for sampling current state of pipeline execution.
DEFAULT_SAMPLING_PERIOD_MS = 200


cdef class StateSampler(object):
  """Tracks time spent in states during pipeline execution."""

  cdef object prefix
  cdef object counter_factory
  cdef int sampling_period_ms

  cdef dict scoped_states_by_name
  cdef list scoped_states_by_index

  cdef bint started
  cdef bint finished
  cdef object sampling_thread

  # This lock guards members that are shared between threads, specificaly
  # finished, scoped_states_by_index, and the nsecs field of each state therein.
  cdef pythread.PyThread_type_lock lock

  cdef public int64_t state_transition_count

  cdef int32_t current_state_index

  def __init__(self, prefix, counter_factory,
      sampling_period_ms=DEFAULT_SAMPLING_PERIOD_MS):

    self.prefix = prefix
    self.counter_factory = counter_factory
    self.sampling_period_ms = sampling_period_ms

    self.lock = pythread.PyThread_allocate_lock()
    self.scoped_states_by_name = {}

    self.current_state_index = 0
    unknown_state = ScopedState(self, 'unknown', self.current_state_index)
    pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
    self.scoped_states_by_index = [unknown_state]
    self.finished = False
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
    cdef int64_t active_elapsed_nsecs
    with nogil:
      while True:
        usleep(self.sampling_period_ms * 1000)
        pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
        try:
          if self.finished:
            break
          elapsed_nsecs = get_nsec_time() - last_nsecs
          # Take an address as we can't create a reference to the scope
          # without the GIL.
          nsecs_ptr = &(<ScopedState>PyList_GET_ITEM(
              self.scoped_states_by_index, self.current_state_index)).nsecs
          nsecs_ptr[0] += elapsed_nsecs
          last_nsecs += elapsed_nsecs

          ###### Compute and set time for active transition. ############
          active_nsecs_ptr = &(<ScopedState>PyList_GET_ITEM(
                 self.scoped_states_by_index,
                 self.current_state_index)).active_nsecs

          completed_ptr = &(<ScopedState>PyList_GET_ITEM(
              self.scoped_states_by_index,
              self.current_state_index)).completed

          # reset the active counter if previously active transition is now
          # complete.
          if completed_ptr[0] > 0:
            active_nsecs_ptr[0] = 0

          is_active = (<ScopedState>PyList_GET_ITEM(
              self.scoped_states_by_index, self.current_state_index)).is_active

          if is_active:
             # Fraction of the state time spent in active transition.
             active_elapsed_nsecs = elapsed_nsecs / (1 + completed_ptr[0])
             active_nsecs_ptr[0] += active_elapsed_nsecs

          # reset the completed count
          completed_ptr[0] = 0

        finally:
          pythread.PyThread_release_lock(self.lock)

  def start(self):
    assert not self.started
    self.started = True
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

  def stop_if_still_running(self):
    if self.started and not self.finished:
      self.stop()

  def get_info(self):
    """Returns StateSamplerInfo with transition statistics."""
    return StateSamplerInfo(
        self.scoped_states_by_index[self.current_state_index].name,
        self.state_transition_count)

  def scoped_state(self, name):
    """Returns a context manager managing transitions for a given state."""
    cdef ScopedState scoped_state = self.scoped_states_by_name.get(name, None)
    if scoped_state is None:
      output_counter = self.counter_factory.get_counter(
          '%s%s-msecs' % (self.prefix,  name), Counter.SUM)
      # TODO: use structured counter names once supported.
      active_output_counter = self.counter_factory.get_counter(
          '%s%s-active-msecs' % (self.prefix,  name), Counter.SUM)
      new_state_index = len(self.scoped_states_by_index)
      scoped_state = ScopedState(self, name, new_state_index, output_counter,
          active_output_counter)
      # Both scoped_states_by_index and scoped_state.nsecs are accessed
      # by the sampling thread; initialize them under the lock.
      pythread.PyThread_acquire_lock(self.lock, pythread.WAIT_LOCK)
      self.scoped_states_by_index.append(scoped_state)
      scoped_state.nsecs = 0
      scoped_state.active_nsecs = 0
      pythread.PyThread_release_lock(self.lock)
      self.scoped_states_by_name[name] = scoped_state
    return scoped_state

  def commit_counters(self):
    """Updates output counters with latest state statistics."""
    for state in self.scoped_states_by_name.values():
      state_msecs = int(1e-6 * state.nsecs)
      state.counter.update(state_msecs - state.counter.value())

      # Resets the active counter if there is no active transition.
      active_state_msecs = 0
      if (<ScopedState>state).is_active:
        active_state_msecs = int(1e-6 * state.active_nsecs)

      state.active_counter.update(
          active_state_msecs - state.active_counter.value())


cdef class ScopedState(object):
  """Context manager class managing transitions for a given sampler state."""

  cdef readonly StateSampler sampler
  cdef readonly int32_t state_index
  cdef readonly object counter
  cdef readonly object active_counter
  cdef readonly object name
  cdef readonly int64_t nsecs
  cdef readonly int64_t active_nsecs
  cdef int32_t old_state_index

  # Number of completed transitions (entered and exited) since the last sample.
  # This is used by the sampling thread to compute the fraction of the time
  # spent in active transition. This value itself is not reported.
  cdef int32_t completed

  # True if there is an active transition (entered but not exited).
  # Note: This assumes that there is only one active transition.
  cdef bint is_active

  def __init__(self, sampler, name, state_index, counter=None,
      active_counter=None):
    self.sampler = sampler
    self.name = name
    self.state_index = state_index
    self.counter = counter
    self.active_counter = active_counter
    self.completed = 0
    self.is_active = False

  cpdef __enter__(self):
    self.old_state_index = self.sampler.current_state_index
    pythread.PyThread_acquire_lock(self.sampler.lock, pythread.WAIT_LOCK)
    self.sampler.current_state_index = self.state_index
    self.is_active = True
    pythread.PyThread_release_lock(self.sampler.lock)
    self.sampler.state_transition_count += 1

  cpdef __exit__(self, unused_exc_type, unused_exc_value, unused_traceback):
    pythread.PyThread_acquire_lock(self.sampler.lock, pythread.WAIT_LOCK)
    self.sampler.current_state_index = self.old_state_index
    self.is_active = False
    self.completed += 1
    pythread.PyThread_release_lock(self.sampler.lock)
    self.sampler.state_transition_count += 1

  def __repr__(self):
    return "ScopedState[%s, %s, %s]" % (self.name, self.state_index, self.nsecs)
