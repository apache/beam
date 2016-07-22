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

from apache_beam.utils.windowed_value cimport WindowedValue

cdef type SideOutputValue, TimestampedValue


cdef class DoFnRunner(object):

  cdef object dofn
  cdef object dofn_process
  cdef object window_fn
  cdef object context   # TODO(robertwb): Make this a DoFnContext
  cdef object tagged_receivers
  cdef object logging_context  # TODO(robertwb): Make this a LoggingContext
  cdef object step_name

  cdef object main_receivers   # TODO(robertwb): Make this a Receiver

  cpdef _process_outputs(self, element, results)


cdef class DoFnContext(object):
  cdef object label
  cdef object state
  cdef WindowedValue windowed_value
  cdef set_element(self, WindowedValue windowed_value)


cdef class Receiver(object):
  cdef receive(self, WindowedValue windowed_value)


cdef class LoggingContext(object):
  # TODO(robertwb): Optimize "with [cdef class]"
  cpdef enter(self)
  cpdef exit(self)
