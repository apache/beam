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
from apache_beam.utils.windowed_value cimport WindowedBatch
from apache_beam.transforms.cy_dataflow_distribution_counter cimport DataflowDistributionCounter

from libc.stdint cimport int64_t


cdef type TaggedOutput, TimestampedValue


cdef class Receiver(object):
  cpdef receive(self, WindowedValue windowed_value)
  cpdef receive_batch(self, WindowedBatch windowed_batch)
  cpdef flush(self)


cdef class MethodWrapper(object):
  cdef public object args
  cdef public object defaults
  cdef public object method_value
  cdef str method_name
  cdef bint has_userstate_arguments
  cdef object state_args_to_replace
  cdef object timer_args_to_replace
  cdef object timestamp_arg_name
  cdef object window_arg_name
  cdef object key_arg_name
  cdef object restriction_provider
  cdef object restriction_provider_arg_name
  cdef public object watermark_estimator_provider
  cdef object watermark_estimator_provider_arg_name
  cdef object dynamic_timer_tag_arg_name
  cdef bint unbounded_per_element


cdef class DoFnSignature(object):
  cdef public MethodWrapper process_method
  cdef public MethodWrapper process_batch_method
  cdef public MethodWrapper start_bundle_method
  cdef public MethodWrapper finish_bundle_method
  cdef public MethodWrapper setup_lifecycle_method
  cdef public MethodWrapper teardown_lifecycle_method
  cdef public MethodWrapper create_watermark_estimator_method
  cdef public MethodWrapper initial_restriction_method
  cdef public MethodWrapper create_tracker_method
  cdef public MethodWrapper split_method
  cdef public object batching_configuration
  cdef public object do_fn
  cdef public object timer_methods
  cdef bint _is_stateful_dofn


cdef class DoFnInvoker(object):
  cdef public DoFnSignature signature
  cdef OutputHandler output_handler
  cdef object user_state_context
  cdef public object bundle_finalizer_param
  cdef object _setup_context_values
  cdef object _bundle_context_values

  cpdef invoke_process(self, WindowedValue windowed_value,
                       restriction=*, watermark_estimator_state=*,
                       additional_args=*, additional_kwargs=*)
  cpdef invoke_start_bundle(self)
  cpdef invoke_finish_bundle(self)
  cpdef invoke_split(self, element, restriction)
  cpdef invoke_initial_restriction(self, element)
  cpdef invoke_create_tracker(self, restriction)


cdef class SimpleInvoker(DoFnInvoker):
  cdef object process_method
  cdef object process_batch_method


cdef class PerWindowInvoker(DoFnInvoker):
  cdef list side_inputs
  cdef DoFnContext context
  cdef list args_for_process
  cdef dict kwargs_for_process
  cdef list placeholders_for_process
  cdef list args_for_process_batch
  cdef dict kwargs_for_process_batch
  cdef list placeholders_for_process_batch
  cdef bint has_windowed_inputs
  cdef bint recalculate_window_args
  cdef bint has_cached_window_args
  cdef bint has_cached_window_batch_args
  cdef object process_method
  cdef object process_batch_method
  cdef bint is_splittable
  cdef object threadsafe_restriction_tracker
  cdef object threadsafe_watermark_estimator
  cdef WindowedValue current_windowed_value
  cdef object restriction
  cdef object watermark_estimator_state
  cdef object current_window_index
  cdef object stop_window_index
  cdef bint is_key_param_required
  cdef object splitting_lock


cdef class DoFnRunner:
  cdef DoFnContext context
  cdef object step_name
  cdef list side_inputs
  cdef DoFnInvoker do_fn_invoker
  cdef public object bundle_finalizer_param
  cdef str transform_id
  cdef object execution_context
  cpdef process(self, WindowedValue windowed_value)


cdef class OutputHandler(object):
  @cython.locals(windowed_value=WindowedValue,
                 output_element_count=int64_t)
  cpdef handle_process_outputs(self, WindowedValue element, results,
                               watermark_estimator=*)

  @cython.locals(windowed_batch=WindowedBatch,
                 output_element_count=int64_t)
  cpdef handle_process_batch_outputs(self, WindowedBatch input_batch, results,
                                     watermark_estimator=*)


cdef class _OutputHandler(OutputHandler):
  cdef object window_fn
  cdef Receiver main_receivers
  cdef object tagged_receivers
  cdef DataflowDistributionCounter per_element_output_counter
  cdef object output_batch_converter
  cdef bint _process_batch_yields_elements
  cdef bint _process_yields_batches

  @cython.locals(windowed_value=WindowedValue,
                 windowed_batch=WindowedBatch,
                 output_element_count=int64_t)
  cpdef handle_process_outputs(self, WindowedValue element, results,
                               watermark_estimator=*)

  @cython.locals(windowed_value=WindowedValue,
                 windowed_batch=WindowedBatch,
                 output_element_count=int64_t)
  cpdef handle_process_batch_outputs(self, WindowedBatch input_batch, results,
                                     watermark_estimator=*)

  @cython.locals(windowed_value=WindowedValue)
  cdef inline WindowedValue _maybe_propagate_windowing_info(self, WindowedValue input_element, result)
  cdef inline tuple _handle_tagged_output(self, result)
  cdef inline _write_value_to_tag(self, tag, WindowedValue windowed_value,
                                  watermark_estimator)
  cdef inline _write_batch_to_tag(self, tag, WindowedBatch windowed_batch,
                                  watermark_estimator)
  cdef inline _verify_batch_output(self, result)

cdef class DoFnContext(object):
  cdef object label
  cdef object state
  cdef WindowedValue windowed_value
  cpdef set_element(self, WindowedValue windowed_value)


cdef class _ReceiverAdapter(Receiver):
  cdef object underlying
