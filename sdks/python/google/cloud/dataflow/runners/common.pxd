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

cdef type SideOutputValue, TimestampedValue, WindowedValue

cdef class DoFnRunner(object):

  cdef object dofn
  cdef object window_fn
  cdef object context
  cdef object tagged_receivers
  cdef object tagged_counters
  cdef object logger
  cdef object step_name

  cdef list main_receivers
  cdef object main_counters

  cpdef _process_outputs(self, element, results)
