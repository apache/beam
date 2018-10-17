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
from apache_beam.transforms.cy_dataflow_distribution_counter cimport DataflowDistributionCounter

from libc.stdint cimport int64_t


cdef type TaggedOutput, TimestampedValue


cdef class Receiver(object):
  cpdef receive(self, WindowedValue windowed_value)


cdef class MethodWrapper(object):
  cdef public object args
  cdef public object defaults
  cdef public object method_value
  cdef bint has_userstate_arguments
  cdef object state_args_to_replace
  cdef object timer_args_to_replace


cdef class DoFnSignature(object):
  cdef public MethodWrapper process_method
  cdef public MethodWrapper start_bundle_method
  cdef public MethodWrapper finish_bundle_method
  cdef public MethodWrapper initial_restriction_method
  cdef public MethodWrapper restriction_coder_method
  cdef public MethodWrapper create_tracker_method
  cdef public MethodWrapper split_method
  cdef public object do_fn
  cdef public object timer_methods
  cdef bint _is_stateful_dofn


cdef class DoFnInvoker(object):
  cdef public DoFnSignature signature
  cdef OutputProcessor output_processor
  cdef object user_state_context

  cpdef invoke_process(self, WindowedValue windowed_value,
                       restriction_tracker=*,
                       OutputProcessor output_processor=*,
                       additional_args=*, additional_kwargs=*)
  cpdef invoke_start_bundle(self)
  cpdef invoke_finish_bundle(self)
  cpdef invoke_split(self, element, restriction)
  cpdef invoke_initial_restriction(self, element)
  cpdef invoke_restriction_coder(self)
  cpdef invoke_create_tracker(self, restriction)


cdef class SimpleInvoker(DoFnInvoker):
  cdef object process_method


cdef class PerWindowInvoker(DoFnInvoker):
  cdef list side_inputs
  cdef DoFnContext context
  cdef list args_for_process
  cdef dict kwargs_for_process
  cdef list placeholders
  cdef bint has_windowed_inputs
  cdef bint cache_globally_windowed_args
  cdef object process_method


cdef class DoFnRunner(Receiver):
  cdef DoFnContext context
  cdef object step_name
  cdef list side_inputs
  cdef DoFnInvoker do_fn_invoker

  cpdef process(self, WindowedValue windowed_value)


cdef class OutputProcessor(object):
  @cython.locals(windowed_value=WindowedValue,
                 output_element_count=int64_t)
  cpdef process_outputs(self, WindowedValue element, results)


cdef class _OutputProcessor(OutputProcessor):
  cdef object window_fn
  cdef Receiver main_receivers
  cdef object tagged_receivers
  cdef DataflowDistributionCounter per_element_output_counter
  @cython.locals(windowed_value=WindowedValue,
                 output_element_count=int64_t)
  cpdef process_outputs(self, WindowedValue element, results)

cdef class DoFnContext(object):
  cdef object label
  cdef object state
  cdef WindowedValue windowed_value
  cpdef set_element(self, WindowedValue windowed_value)


cdef class _ReceiverAdapter(Receiver):
  cdef object underlying
