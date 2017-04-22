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

from apache_beam.utils.windowed_value cimport WindowedValue
from apache_beam.metrics.execution cimport ScopedMetricsContainer


cdef type OutputValue, TimestampedValue


cdef class Receiver(object):
  cpdef receive(self, WindowedValue windowed_value)


cdef class DoFnRunner(Receiver):

  cdef object dofn
  cdef object dofn_process
  cdef object window_fn
  cdef DoFnContext context
  cdef object tagged_receivers
  cdef LoggingContext logging_context
  cdef object step_name
  cdef list args
  cdef dict kwargs
  cdef ScopedMetricsContainer scoped_metrics_container
  cdef list side_inputs
  cdef bint has_windowed_inputs
  cdef list placeholders
  cdef bint use_simple_invoker

  cdef Receiver main_receivers

  cpdef process(self, WindowedValue element)
  cdef _dofn_invoker(self, WindowedValue element)
  cdef _dofn_simple_invoker(self, WindowedValue element)
  cdef _dofn_per_window_invoker(self, WindowedValue element)

  @cython.locals(windowed_value=WindowedValue)
  cpdef _process_outputs(self, WindowedValue element, results)


cdef class DoFnContext(object):
  cdef object label
  cdef object state
  cdef WindowedValue windowed_value
  cpdef set_element(self, WindowedValue windowed_value)


cdef class LoggingContext(object):
  # TODO(robertwb): Optimize "with [cdef class]"
  cpdef enter(self)
  cpdef exit(self)


cdef class _LoggingContextAdapter(LoggingContext):
  cdef object underlying


cdef class _ReceiverAdapter(Receiver):
  cdef object underlying
