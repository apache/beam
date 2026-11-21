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


cdef type Timestamp


cdef list _BYTE_TO_PANE_INFO

@cython.final
cdef class PaneInfo(object):
  cdef readonly bint _is_first
  cdef readonly bint _is_last
  cdef readonly int _timing
  cdef readonly int _index
  cdef readonly int _nonspeculative_index
  cdef readonly unsigned char _encoded_byte

@cython.final
cdef class WindowedValue(object):
  cdef public object value
  cdef public object windows
  cdef public object pane_info
  cdef public int64_t timestamp_micros
  cdef object timestamp_object

  cpdef WindowedValue with_value(self, new_value)

cdef class WindowedBatch(object):
  cpdef WindowedBatch with_values(self, object new_values)

cdef class HomogeneousWindowedBatch(WindowedBatch):
  cdef public WindowedValue _wv

  cpdef WindowedBatch with_values(self, object new_values)

@cython.locals(wv=WindowedValue)
cpdef WindowedValue create(
  object value, int64_t timestamp_micros, object windows, object pane_info=*)


cdef class _IntervalWindowBase(object):
  cdef object _start_object
  cdef readonly int64_t _start_micros
  cdef object _end_object
  cdef readonly int64_t _end_micros
