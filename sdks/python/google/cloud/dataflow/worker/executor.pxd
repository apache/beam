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

cdef class Operation(object):
  cdef public spec
  cdef public counter_factory
  cdef public list receivers
  cdef public list counters
  cdef readonly bint debug_logging_enabled

  cdef public step_name  # initialized lazily

  cpdef start(self)
  cpdef process(self, windowed_value)
  cpdef finish(self)

  @cython.locals(receiver=Operation)
  cpdef output(self, windowed_value, object coder=*, int output_index=*)

cdef class ReadOperation(Operation):
  cdef object _current_progress
  cdef object _reader

cdef class DoOperation(Operation):
  cdef object state
  cdef object context
  cdef object dofn_runner

cdef class CombineOperation(Operation):
  cdef object phased_combine_fn

cdef class ShuffleWriteOperation(Operation):
  cdef object shuffle_sink
  cdef object writer
  cdef object _write_coder
  cdef bint is_ungrouped

cdef class GroupedShuffleReadOperation(Operation):
  cdef object shuffle_source
  cdef object _reader

cdef class UngroupedShuffleReadOperation(Operation):
  cdef object shuffle_source
  cdef object _reader

cdef class FlattenOperation(Operation):
  pass

cdef class ReifyTimestampAndWindowsOperation(Operation):
  pass

cdef class BatchGroupAlsoByWindowsOperation(Operation):
  cdef object windowing
  cdef object phased_combine_fn

cdef class StreamingGroupAlsoByWindowsOperation(Operation):
  cdef object windowing
  cdef object phased_combine_fn


cdef class PGBKCVOperation(Operation):
  cdef public object combine_fn
  cdef dict table
  cdef long max_keys
  cdef long key_count

  cpdef output_key(self, tuple wkey, value)
