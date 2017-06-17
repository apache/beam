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

"""A PipelineRunner using the SDK harness.
"""
import collections
import json
import logging
import Queue as queue
import threading

from concurrent import futures
import grpc

import apache_beam as beam
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.transforms.window import GlobalWindows
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.portability import maptask_executor_runner
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import sdk_worker

# This module is experimental. No backwards-compatibility guarantees.


def streaming_rpc_handler(cls, method_name):
  """Un-inverts the flow of control between the runner and the sdk harness."""

  class StreamingRpcHandler(cls):

    _DONE = object()

    def __init__(self):
      self._push_queue = queue.Queue()
      self._pull_queue = queue.Queue()
      setattr(self, method_name, self.run)
      self._read_thread = threading.Thread(target=self._read)

    def run(self, iterator, context):
      self._inputs = iterator
      # Note: We only support one client for now.
      self._read_thread.start()
      while True:
        to_push = self._push_queue.get()
        if to_push is self._DONE:
          return
        yield to_push

    def _read(self):
      for data in self._inputs:
        self._pull_queue.put(data)

    def push(self, item):
      self._push_queue.put(item)

    def pull(self, timeout=None):
      return self._pull_queue.get(timeout=timeout)

    def empty(self):
      return self._pull_queue.empty()

    def done(self):
      self.push(self._DONE)
      self._read_thread.join()

  return StreamingRpcHandler()


class OldeSourceSplittableDoFn(beam.DoFn):
  """A DoFn that reads and emits an entire source.
  """

  # TODO(robertwb): Make this a full SDF with progress splitting, etc.
  def process(self, source):
    if isinstance(source, iobase.SourceBundle):
      for value in source.source.read(source.source.get_range_tracker(
          source.start_position, source.stop_position)):
        yield value
    else:
      # Dataflow native source
      with source.reader() as reader:
        for value in reader:
          yield value


# See DataflowRunner._pardo_fn_data
OLDE_SOURCE_SPLITTABLE_DOFN_DATA = pickler.dumps(
    (OldeSourceSplittableDoFn(), (), {}, [],
     beam.transforms.core.Windowing(GlobalWindows())))


class FnApiRunner(maptask_executor_runner.MapTaskExecutorRunner):

  def __init__(self):
    super(FnApiRunner, self).__init__()
    self._last_uid = -1

  def has_metrics_support(self):
    return False

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def _map_task_registration(self, map_task, state_handler,
                             data_operation_spec):
    input_data = {}
    runner_sinks = {}
    transforms = []
    transform_index_to_id = {}

    # Maps coders to new coder objects and references.
    coders = {}

    def coder_id(coder):
      if coder not in coders:
        coders[coder] = beam_fn_api_pb2.Coder(
            function_spec=sdk_worker.pack_function_spec_data(
                json.dumps(coder.as_cloud_object()),
                sdk_worker.PYTHON_CODER_URN, id=self._next_uid()))

      return coders[coder].function_spec.id

    def output_tags(op):
      return getattr(op, 'output_tags', ['out'])

    def as_target(op_input):
      input_op_index, input_output_index = op_input
      input_op = map_task[input_op_index][1]
      return {
          'ignored_input_tag':
              beam_fn_api_pb2.Target.List(target=[
                  beam_fn_api_pb2.Target(
                      primitive_transform_reference=transform_index_to_id[
                          input_op_index],
                      name=output_tags(input_op)[input_output_index])
              ])
      }

    def outputs(op):
      return {
          tag: beam_fn_api_pb2.PCollection(coder_reference=coder_id(coder))
          for tag, coder in zip(output_tags(op), op.output_coders)
      }

    for op_ix, (stage_name, operation) in enumerate(map_task):
      transform_id = transform_index_to_id[op_ix] = self._next_uid()
      if isinstance(operation, operation_specs.WorkerInMemoryWrite):
        # Write this data back to the runner.
        fn = beam_fn_api_pb2.FunctionSpec(urn=sdk_worker.DATA_OUTPUT_URN,
                                          id=self._next_uid())
        if data_operation_spec:
          fn.data.Pack(data_operation_spec)
        inputs = as_target(operation.input)
        side_inputs = {}
        runner_sinks[(transform_id, 'out')] = operation

      elif isinstance(operation, operation_specs.WorkerRead):
        # A Read is either translated to a direct injection of windowed values
        # into the sdk worker, or an injection of the source object into the
        # sdk worker as data followed by an SDF that reads that source.
        if (isinstance(operation.source.source,
                       maptask_executor_runner.InMemorySource)
            and isinstance(operation.source.source.default_output_coder(),
                           WindowedValueCoder)):
          output_stream = create_OutputStream()
          element_coder = (
              operation.source.source.default_output_coder().get_impl())
          # Re-encode the elements in the nested context and
          # concatenate them together
          for element in operation.source.source.read(None):
            element_coder.encode_to_stream(element, output_stream, True)
          target_name = self._next_uid()
          input_data[(transform_id, target_name)] = output_stream.get()
          fn = beam_fn_api_pb2.FunctionSpec(urn=sdk_worker.DATA_INPUT_URN,
                                            id=self._next_uid())
          if data_operation_spec:
            fn.data.Pack(data_operation_spec)
          inputs = {target_name: beam_fn_api_pb2.Target.List()}
          side_inputs = {}
        else:
          # Read the source object from the runner.
          source_coder = beam.coders.DillCoder()
          input_transform_id = self._next_uid()
          output_stream = create_OutputStream()
          source_coder.get_impl().encode_to_stream(
              GlobalWindows.windowed_value(operation.source),
              output_stream,
              True)
          target_name = self._next_uid()
          input_data[(input_transform_id, target_name)] = output_stream.get()
          input_ptransform = beam_fn_api_pb2.PrimitiveTransform(
              id=input_transform_id,
              function_spec=beam_fn_api_pb2.FunctionSpec(
                  urn=sdk_worker.DATA_INPUT_URN,
                  id=self._next_uid()),
              # TODO(robertwb): Possible name collision.
              step_name=stage_name + '/inject_source',
              inputs={target_name: beam_fn_api_pb2.Target.List()},
              outputs={
                  'out':
                      beam_fn_api_pb2.PCollection(
                          coder_reference=coder_id(source_coder))
              })
          if data_operation_spec:
            input_ptransform.function_spec.data.Pack(data_operation_spec)
          transforms.append(input_ptransform)

          # Read the elements out of the source.
          fn = sdk_worker.pack_function_spec_data(
              OLDE_SOURCE_SPLITTABLE_DOFN_DATA,
              sdk_worker.PYTHON_DOFN_URN,
              id=self._next_uid())
          inputs = {
              'ignored_input_tag':
                  beam_fn_api_pb2.Target.List(target=[
                      beam_fn_api_pb2.Target(
                          primitive_transform_reference=input_transform_id,
                          name='out')
                  ])
          }
          side_inputs = {}

      elif isinstance(operation, operation_specs.WorkerDoFn):
        fn = sdk_worker.pack_function_spec_data(
            operation.serialized_fn,
            sdk_worker.PYTHON_DOFN_URN,
            id=self._next_uid())
        inputs = as_target(operation.input)
        # Store the contents of each side input for state access.
        for si in operation.side_inputs:
          assert isinstance(si.source, iobase.BoundedSource)
          element_coder = si.source.default_output_coder()
          view_id = self._next_uid()
          # TODO(robertwb): Actually flesh out the ViewFn API.
          side_inputs[si.tag] = beam_fn_api_pb2.SideInput(
              view_fn=sdk_worker.serialize_and_pack_py_fn(
                  element_coder, urn=sdk_worker.PYTHON_ITERABLE_VIEWFN_URN,
                  id=view_id))
          # Re-encode the elements in the nested context and
          # concatenate them together
          output_stream = create_OutputStream()
          for element in si.source.read(
              si.source.get_range_tracker(None, None)):
            element_coder.get_impl().encode_to_stream(
                element, output_stream, True)
          elements_data = output_stream.get()
          state_key = beam_fn_api_pb2.StateKey(key=view_id)
          state_handler.Clear(state_key)
          state_handler.Append(state_key, elements_data)

      elif isinstance(operation, operation_specs.WorkerFlatten):
        fn = sdk_worker.pack_function_spec_data(
            operation.serialized_fn,
            sdk_worker.IDENTITY_DOFN_URN,
            id=self._next_uid())
        inputs = {
            'ignored_input_tag':
                beam_fn_api_pb2.Target.List(target=[
                    beam_fn_api_pb2.Target(
                        primitive_transform_reference=transform_index_to_id[
                            input_op_index],
                        name=output_tags(map_task[input_op_index][1])[
                            input_output_index])
                    for input_op_index, input_output_index in operation.inputs
                ])
        }
        side_inputs = {}

      else:
        raise TypeError(operation)

      ptransform = beam_fn_api_pb2.PrimitiveTransform(
          id=transform_id,
          function_spec=fn,
          step_name=stage_name,
          inputs=inputs,
          side_inputs=side_inputs,
          outputs=outputs(operation))
      transforms.append(ptransform)

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(), coders=coders.values(),
        primitive_transform=transforms)
    return beam_fn_api_pb2.InstructionRequest(
        instruction_id=self._next_uid(),
        register=beam_fn_api_pb2.RegisterRequest(
            process_bundle_descriptor=[process_bundle_descriptor
                                      ])), runner_sinks, input_data

  def _run_map_task(
      self, map_task, control_handler, state_handler, data_plane_handler,
      data_operation_spec):
    registration, sinks, input_data = self._map_task_registration(
        map_task, state_handler, data_operation_spec)
    control_handler.push(registration)
    process_bundle = beam_fn_api_pb2.InstructionRequest(
        instruction_id=self._next_uid(),
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_reference=registration.register.
            process_bundle_descriptor[0].id))

    for (transform_id, name), elements in input_data.items():
      data_out = data_plane_handler.output_stream(
          process_bundle.instruction_id, beam_fn_api_pb2.Target(
              primitive_transform_reference=transform_id, name=name))
      data_out.write(elements)
      data_out.close()

    control_handler.push(process_bundle)
    while True:
      result = control_handler.pull()
      if result.instruction_id == process_bundle.instruction_id:
        if result.error:
          raise RuntimeError(result.error)
        expected_targets = [
            beam_fn_api_pb2.Target(primitive_transform_reference=transform_id,
                                   name=output_name)
            for (transform_id, output_name), _ in sinks.items()]
        for output in data_plane_handler.input_elements(
            process_bundle.instruction_id, expected_targets):
          target_tuple = (
              output.target.primitive_transform_reference, output.target.name)
          if target_tuple not in sinks:
            # Unconsumed output.
            continue
          sink_op = sinks[target_tuple]
          coder = sink_op.output_coders[0]
          input_stream = create_InputStream(output.data)
          elements = []
          while input_stream.size() > 0:
            elements.append(coder.get_impl().decode_from_stream(
                input_stream, True))
          if not sink_op.write_windowed_values:
            elements = [e.value for e in elements]
          for e in elements:
            sink_op.output_buffer.append(e)
        return

  def execute_map_tasks(self, ordered_map_tasks, direct=True):
    if direct:
      controller = FnApiRunner.DirectController()
    else:
      controller = FnApiRunner.GrpcController()

    try:
      for _, map_task in ordered_map_tasks:
        logging.info('Running %s', map_task)
        self._run_map_task(
            map_task, controller.control_handler, controller.state_handler,
            controller.data_plane_handler, controller.data_operation_spec())
    finally:
      controller.close()

  class SimpleState(object):  # TODO(robertwb): Inherit from GRPC servicer.

    def __init__(self):
      self._all = collections.defaultdict(list)

    def Get(self, state_key):
      return beam_fn_api_pb2.Elements.Data(
          data=''.join(self._all[self._to_key(state_key)]))

    def Append(self, state_key, data):
      self._all[self._to_key(state_key)].extend(data)

    def Clear(self, state_key):
      try:
        del self._all[self._to_key(state_key)]
      except KeyError:
        pass

    @staticmethod
    def _to_key(state_key):
      return state_key.window, state_key.key

  class DirectController(object):
    """An in-memory controller for fn API control, state and data planes."""

    def __init__(self):
      self._responses = []
      self.state_handler = FnApiRunner.SimpleState()
      self.control_handler = self
      self.data_plane_handler = data_plane.InMemoryDataChannel()
      self.worker = sdk_worker.SdkWorker(
          self.state_handler, data_plane.InMemoryDataChannelFactory(
              self.data_plane_handler.inverse()))

    def push(self, request):
      logging.info('CONTROL REQUEST %s', request)
      response = self.worker.do_instruction(request)
      logging.info('CONTROL RESPONSE %s', response)
      self._responses.append(response)

    def pull(self):
      return self._responses.pop(0)

    def done(self):
      pass

    def close(self):
      pass

    def data_operation_spec(self):
      return None

  class GrpcController(object):
    """An grpc based controller for fn API control, state and data planes."""

    def __init__(self):
      self.state_handler = FnApiRunner.SimpleState()
      self.control_server = grpc.server(
          futures.ThreadPoolExecutor(max_workers=10))
      self.control_port = self.control_server.add_insecure_port('[::]:0')

      self.data_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
      self.data_port = self.data_server.add_insecure_port('[::]:0')

      self.control_handler = streaming_rpc_handler(
          beam_fn_api_pb2.BeamFnControlServicer, 'Control')
      beam_fn_api_pb2.add_BeamFnControlServicer_to_server(
          self.control_handler, self.control_server)

      self.data_plane_handler = data_plane.GrpcServerDataChannel()
      beam_fn_api_pb2.add_BeamFnDataServicer_to_server(
          self.data_plane_handler, self.data_server)

      logging.info('starting control server on port %s', self.control_port)
      logging.info('starting data server on port %s', self.data_port)
      self.data_server.start()
      self.control_server.start()

      self.worker = sdk_worker.SdkHarness(
          grpc.insecure_channel('localhost:%s' % self.control_port))
      self.worker_thread = threading.Thread(target=self.worker.run)
      logging.info('starting worker')
      self.worker_thread.start()

    def data_operation_spec(self):
      url = 'localhost:%s' % self.data_port
      remote_grpc_port = beam_fn_api_pb2.RemoteGrpcPort()
      remote_grpc_port.api_service_descriptor.url = url
      return remote_grpc_port

    def close(self):
      self.control_handler.done()
      self.worker_thread.join()
      self.data_plane_handler.close()
      self.control_server.stop(5).wait()
      self.data_server.stop(5).wait()
