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
import base64
import collections
import json
import logging
import Queue as queue
import threading

from concurrent import futures
from google.protobuf import wrappers_pb2
import grpc

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.transforms.window import GlobalWindows
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import maptask_executor_runner
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import sdk_worker
from apache_beam.utils import proto_utils

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
    input_data, side_input_data, runner_sinks, process_bundle_descriptor = (
        self._map_task_to_protos(map_task, data_operation_spec))
    # Side inputs will be accessed over the state API.
    for key, elements_data in side_input_data.items():
      state_key = beam_fn_api_pb2.StateKey.MultimapSideInput(key=key)
      state_handler.Clear(state_key)
      state_handler.Append(state_key, [elements_data])
    return beam_fn_api_pb2.InstructionRequest(
        instruction_id=self._next_uid(),
        register=beam_fn_api_pb2.RegisterRequest(
            process_bundle_descriptor=[process_bundle_descriptor])
        ), runner_sinks, input_data

  def _map_task_to_protos(self, map_task, data_operation_spec):
    input_data = {}
    side_input_data = {}
    runner_sinks = {}

    context = pipeline_context.PipelineContext()
    transform_protos = {}
    used_pcollections = {}

    def uniquify(*names):
      # An injective mapping from string* to string.
      return ':'.join("%s:%d" % (name, len(name)) for name in names)

    def pcollection_id(op_ix, out_ix):
      if (op_ix, out_ix) not in used_pcollections:
        used_pcollections[op_ix, out_ix] = uniquify(
            map_task[op_ix][0], 'out', str(out_ix))
      return used_pcollections[op_ix, out_ix]

    def get_inputs(op):
      if hasattr(op, 'inputs'):
        inputs = op.inputs
      elif hasattr(op, 'input'):
        inputs = [op.input]
      else:
        inputs = []
      return {'in%s' % ix: pcollection_id(*input)
              for ix, input in enumerate(inputs)}

    def get_outputs(op_ix):
      op = map_task[op_ix][1]
      return {tag: pcollection_id(op_ix, out_ix)
              for out_ix, tag in enumerate(getattr(op, 'output_tags', ['out']))}

    for op_ix, (stage_name, operation) in enumerate(map_task):
      transform_id = uniquify(stage_name)

      if isinstance(operation, operation_specs.WorkerInMemoryWrite):
        # Write this data back to the runner.
        runner_sinks[(transform_id, 'out')] = operation
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=sdk_worker.DATA_OUTPUT_URN,
            parameter=proto_utils.pack_Any(data_operation_spec))

      elif isinstance(operation, operation_specs.WorkerRead):
        # A Read from an in-memory source is done over the data plane.
        if (isinstance(operation.source.source,
                       maptask_executor_runner.InMemorySource)
            and isinstance(operation.source.source.default_output_coder(),
                           WindowedValueCoder)):
          input_data[(transform_id, 'input')] = self._reencode_elements(
              operation.source.source.read(None),
              operation.source.source.default_output_coder())
          transform_spec = beam_runner_api_pb2.FunctionSpec(
              urn=sdk_worker.DATA_INPUT_URN,
              parameter=proto_utils.pack_Any(data_operation_spec))

        else:
          # Otherwise serialize the source and execute it there.
          # TODO: Use SDFs with an initial impulse.
          # The Dataflow runner harness strips the base64 encoding. do the same
          # here until we get the same thing back that we sent in.
          transform_spec = beam_runner_api_pb2.FunctionSpec(
              urn=sdk_worker.PYTHON_SOURCE_URN,
              parameter=proto_utils.pack_Any(
                  wrappers_pb2.BytesValue(
                      value=base64.b64decode(
                          pickler.dumps(operation.source.source)))))

      elif isinstance(operation, operation_specs.WorkerDoFn):
        # Record the contents of each side input for access via the state api.
        side_input_extras = []
        for si in operation.side_inputs:
          assert isinstance(si.source, iobase.BoundedSource)
          element_coder = si.source.default_output_coder()
          # TODO(robertwb): Actually flesh out the ViewFn API.
          side_input_extras.append((si.tag, element_coder))
          side_input_data[sdk_worker.side_input_tag(transform_id, si.tag)] = (
              self._reencode_elements(
                  si.source.read(si.source.get_range_tracker(None, None)),
                  element_coder))
        augmented_serialized_fn = pickler.dumps(
            (operation.serialized_fn, side_input_extras))
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=sdk_worker.PYTHON_DOFN_URN,
            parameter=proto_utils.pack_Any(
                wrappers_pb2.BytesValue(value=augmented_serialized_fn)))

      elif isinstance(operation, operation_specs.WorkerFlatten):
        # Flatten is nice and simple.
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=sdk_worker.IDENTITY_DOFN_URN)

      else:
        raise NotImplementedError(operation)

      transform_protos[transform_id] = beam_runner_api_pb2.PTransform(
          unique_name=stage_name,
          spec=transform_spec,
          inputs=get_inputs(operation),
          outputs=get_outputs(op_ix))

    pcollection_protos = {
        name: beam_runner_api_pb2.PCollection(
            unique_name=name,
            coder_id=context.coders.get_id(
                map_task[op_id][1].output_coders[out_id]))
        for (op_id, out_id), name in used_pcollections.items()
    }
    # Must follow creation of pcollection_protos to capture used coders.
    context_proto = context.to_runner_api()
    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms=transform_protos,
        pcollections=pcollection_protos,
        codersyyy=dict(context_proto.coders.items()),
        windowing_strategies=dict(context_proto.windowing_strategies.items()),
        environments=dict(context_proto.environments.items()))
    return input_data, side_input_data, runner_sinks, process_bundle_descriptor

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

  @staticmethod
  def _reencode_elements(elements, element_coder):
    output_stream = create_OutputStream()
    for element in elements:
      element_coder.get_impl().encode_to_stream(element, output_stream, True)
    return output_stream.get()
