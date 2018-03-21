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
# distutils: language=c++
# distutils: extra_compile_args=['-std=c++11']

cimport cython
from libc.stdint cimport int64_t
from libcpp.deque cimport deque
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector


ctypedef char* CharPtr

"""Tracking object for the execution of a single input element in a step.
Each ElementExecution instance represents a distinct element. Instances
compare using reference(address) equality rather than value equality.
"""
cdef struct ElementExecution:
  char* operation_name

ctypedef ElementExecution* ElementExecutionPtr

"""Journal entry.
IDLE execution: execution_ptr = NULL.
"""
cdef struct SnapshottedExecution:
  int64_t snapshot
  ElementExecutionPtr execution_ptr

ctypedef SnapshottedExecution* SnapshottedExecutionPtr

cdef class Journal(object):
  cdef deque[SnapshottedExecutionPtr] journal
  cdef int64_t max_snapshot
  cdef void add_journal(self, ElementExecutionPtr execution_ptr,
                        int64_t snapshot)

cdef class ReaderWriterState(object):
  cdef Journal execution_journal
  cdef Journal done_journal
  cdef int64_t latest_snapshot

cdef class ExecutionJournalReader(object):
  cdef ReaderWriterState shared_state
  cdef unordered_map[ElementExecutionPtr, int64_t] execution_duration
  cdef void take_sample(self, int64_t sample_time,
      unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil
  cdef void attribute_processing_time(self, int64_t sample_time,
                                      int64_t latest_snapshot) nogil
  cdef void update_counter_cache(self, int64_t latest_snapshot,
      unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil

cdef class ExecutionJournalWriter(object):
  cdef ReaderWriterState shared_state
  cdef deque[ElementExecutionPtr] execution_stack
  cdef void add_execution(self, ElementExecutionPtr execution_ptr)
  cdef void start_processing(self, char* operation_name)
  cdef void done_processing(self)

cdef class DataflowElementExecutionTracker(object):
  cdef ReaderWriterState shared_state
  cdef ExecutionJournalWriter execution_writer
  cdef ExecutionJournalReader execution_reader
  cdef unordered_map[CharPtr, vector[int64_t]] counter_cache
  cdef unordered_map[CharPtr, int64_t] cache_start_index
  cdef void enter(self, char* operation_name)
  cdef void exit(self)
  cdef void take_sample(self, int64_t nanos_sampling_duration) nogil
  cpdef void enter_for_test(self, char* operation_name)
  cpdef void exit_for_test(self)
  cpdef void take_sample_for_test(self, int64_t nanos_sampling_duration)
  cpdef report_counter(self, counter_factory)
