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

"""SDK harness for executing Python Fns via the Fn API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import logging
import Queue as queue
import threading
import traceback
import zlib

import dill
from google.protobuf import wrappers_pb2

from apache_beam.coders import coder_impl
from apache_beam.coders import WindowedValueCoder
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.runners.dataflow.native_io import iobase as native_iobase
from apache_beam.utils import counters
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations

# This module is experimental. No backwards-compatibility guarantees.


try:
  from apache_beam.runners.worker import statesampler
except ImportError:
  from apache_beam.runners.worker import statesampler_fake as statesampler
from apache_beam.runners.worker.data_plane import GrpcClientDataChannelFactory


DATA_INPUT_URN = 'urn:org.apache.beam:source:runner:0.1'
DATA_OUTPUT_URN = 'urn:org.apache.beam:sink:runner:0.1'
IDENTITY_DOFN_URN = 'urn:org.apache.beam:dofn:identity:0.1'
PYTHON_ITERABLE_VIEWFN_URN = 'urn:org.apache.beam:viewfn:iterable:python:0.1'
PYTHON_CODER_URN = 'urn:org.apache.beam:coder:python:0.1'
# TODO(vikasrk): Fix this once runner sends appropriate python urns.
PYTHON_DOFN_URN = 'urn:org.apache.beam:dofn:java:0.1'
PYTHON_SOURCE_URN = 'urn:org.apache.beam:source:java:0.1'


class RunnerIOOperation(operations.Operation):
  """Common baseclass for runner harness IO operations."""

  def __init__(self, operation_name, step_name, consumers, counter_factory,
               state_sampler, windowed_coder, target, data_channel):
    super(RunnerIOOperation, self).__init__(
        operation_name, None, counter_factory, state_sampler)
    self.windowed_coder = windowed_coder
    self.step_name = step_name
    # target represents the consumer for the bytes in the data plane for a
    # DataInputOperation or a producer of these bytes for a DataOutputOperation.
    self.target = target
    self.data_channel = data_channel
    for _, consumer_ops in consumers.items():
      for consumer in consumer_ops:
        self.add_receiver(consumer, 0)


class DataOutputOperation(RunnerIOOperation):
  """A sink-like operation that gathers outputs to be sent back to the runner.
  """

  def set_output_stream(self, output_stream):
    self.output_stream = output_stream

  def process(self, windowed_value):
    self.windowed_coder.get_impl().encode_to_stream(
        windowed_value, self.output_stream, True)

  def finish(self):
    self.output_stream.close()
    super(DataOutputOperation, self).finish()


class DataInputOperation(RunnerIOOperation):
  """A source-like operation that gathers input from the runner.
  """

  def __init__(self, operation_name, step_name, consumers, counter_factory,
               state_sampler, windowed_coder, input_target, data_channel):
    super(DataInputOperation, self).__init__(
        operation_name, step_name, consumers, counter_factory, state_sampler,
        windowed_coder, target=input_target, data_channel=data_channel)
    # We must do this manually as we don't have a spec or spec.output_coders.
    self.receivers = [
        operations.ConsumerSet(self.counter_factory, self.step_name, 0,
                               consumers.itervalues().next(),
                               self.windowed_coder)]

  def process(self, windowed_value):
    self.output(windowed_value)

  def process_encoded(self, encoded_windowed_values):
    input_stream = coder_impl.create_InputStream(encoded_windowed_values)
    while input_stream.size() > 0:
      decoded_value = self.windowed_coder.get_impl().decode_from_stream(
          input_stream, True)
      self.output(decoded_value)


# TODO(robertwb): Revise side input API to not be in terms of native sources.
# This will enable lookups, but there's an open question as to how to handle
# custom sources without forcing intermediate materialization.  This seems very
# related to the desire to inject key and window preserving [Splittable]DoFns
# into the view computation.
class SideInputSource(native_iobase.NativeSource,
                      native_iobase.NativeSourceReader):
  """A 'source' for reading side inputs via state API calls.
  """

  def __init__(self, state_handler, state_key, coder):
    self._state_handler = state_handler
    self._state_key = state_key
    self._coder = coder

  def reader(self):
    return self

  @property
  def returns_windowed_values(self):
    return True

  def __enter__(self):
    return self

  def __exit__(self, *exn_info):
    pass

  def __iter__(self):
    # TODO(robertwb): Support pagination.
    input_stream = coder_impl.create_InputStream(
        self._state_handler.Get(self._state_key).data)
    while input_stream.size() > 0:
      yield self._coder.get_impl().decode_from_stream(input_stream, True)


def unpack_and_deserialize_py_fn(function_spec):
  """Returns unpacked and deserialized object from function spec proto."""
  return pickler.loads(unpack_function_spec_data(function_spec))


def unpack_function_spec_data(function_spec):
  """Returns unpacked data from function spec proto."""
  data = wrappers_pb2.BytesValue()
  function_spec.data.Unpack(data)
  return data.value


# pylint: disable=redefined-builtin
def serialize_and_pack_py_fn(fn, urn, id=None):
  """Returns serialized and packed function in a function spec proto."""
  return pack_function_spec_data(pickler.dumps(fn), urn, id)
# pylint: enable=redefined-builtin


# pylint: disable=redefined-builtin
def pack_function_spec_data(value, urn, id=None):
  """Returns packed data in a function spec proto."""
  data = wrappers_pb2.BytesValue(value=value)
  fn_proto = beam_fn_api_pb2.FunctionSpec(urn=urn)
  fn_proto.data.Pack(data)
  if id:
    fn_proto.id = id
  return fn_proto
# pylint: enable=redefined-builtin


# TODO(vikasrk): move this method to ``coders.py`` in the SDK.
def load_compressed(compressed_data):
  """Returns a decompressed and deserialized python object."""
  # Note: SDK uses ``pickler.dumps`` to serialize certain python objects
  # (like sources), which involves serialization, compression and base64
  # encoding. We cannot directly use ``pickler.loads`` for
  # deserialization, as the runner would have already base64 decoded the
  # data. So we only need to decompress and deserialize.

  data = zlib.decompress(compressed_data)
  try:
    return dill.loads(data)
  except Exception:          # pylint: disable=broad-except
    dill.dill._trace(True)   # pylint: disable=protected-access
    return dill.loads(data)
  finally:
    dill.dill._trace(False)  # pylint: disable=protected-access


class SdkHarness(object):

  def __init__(self, control_channel):
    self._control_channel = control_channel
    self._data_channel_factory = GrpcClientDataChannelFactory()

  def run(self):
    contol_stub = beam_fn_api_pb2.BeamFnControlStub(self._control_channel)
    # TODO(robertwb): Wire up to new state api.
    state_stub = None
    self.worker = SdkWorker(state_stub, self._data_channel_factory)

    responses = queue.Queue()
    no_more_work = object()

    def get_responses():
      while True:
        response = responses.get()
        if response is no_more_work:
          return
        yield response

    def process_requests():
      for work_request in contol_stub.Control(get_responses()):
        logging.info('Got work %s', work_request.instruction_id)
        try:
          response = self.worker.do_instruction(work_request)
        except Exception:  # pylint: disable=broad-except
          response = beam_fn_api_pb2.InstructionResponse(
              instruction_id=work_request.instruction_id,
              error=traceback.format_exc())
        responses.put(response)
    t = threading.Thread(target=process_requests)
    t.start()
    t.join()
    # get_responses may be blocked on responses.get(), but we need to return
    # control to its caller.
    responses.put(no_more_work)
    self._data_channel_factory.close()
    logging.info('Done consuming work.')


class SdkWorker(object):

  def __init__(self, state_handler, data_channel_factory):
    self.fns = {}
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory

  def do_instruction(self, request):
    request_type = request.WhichOneof('request')
    if request_type:
      # E.g. if register is set, this will construct
      # InstructionResponse(register=self.register(request.register))
      return beam_fn_api_pb2.InstructionResponse(**{
          'instruction_id': request.instruction_id,
          request_type: getattr(self, request_type)
                        (getattr(request, request_type), request.instruction_id)
      })
    else:
      raise NotImplementedError

  def register(self, request, unused_instruction_id=None):
    for process_bundle_descriptor in request.process_bundle_descriptor:
      self.fns[process_bundle_descriptor.id] = process_bundle_descriptor
      for p_transform in list(process_bundle_descriptor.primitive_transform):
        self.fns[p_transform.function_spec.id] = p_transform.function_spec
    return beam_fn_api_pb2.RegisterResponse()

  def initial_source_split(self, request, unused_instruction_id=None):
    source_spec = self.fns[request.source_reference]
    assert source_spec.urn == PYTHON_SOURCE_URN
    source_bundle = unpack_and_deserialize_py_fn(
        self.fns[request.source_reference])
    splits = source_bundle.source.split(request.desired_bundle_size_bytes,
                                        source_bundle.start_position,
                                        source_bundle.stop_position)
    response = beam_fn_api_pb2.InitialSourceSplitResponse()
    response.splits.extend([
        beam_fn_api_pb2.SourceSplit(
            source=serialize_and_pack_py_fn(split, PYTHON_SOURCE_URN),
            relative_size=split.weight,
        )
        for split in splits
    ])
    return response

  def create_execution_tree(self, descriptor):
    # TODO(vikasrk): Add an id field to Coder proto and use that instead.
    coders = {coder.function_spec.id: operation_specs.get_coder_from_spec(
        json.loads(unpack_function_spec_data(coder.function_spec)))
              for coder in descriptor.coders}

    counter_factory = counters.CounterFactory()
    # TODO(robertwb): Figure out the correct prefix to use for output counters
    # from StateSampler.
    state_sampler = statesampler.StateSampler(
        'fnapi-step%s-' % descriptor.id, counter_factory)
    consumers = collections.defaultdict(lambda: collections.defaultdict(list))
    ops_by_id = {}
    reversed_ops = []

    for transform in reversed(descriptor.primitive_transform):
      # TODO(robertwb): Figure out how to plumb through the operation name (e.g.
      # "s3") from the service through the FnAPI so that msec counters can be
      # reported and correctly plumbed through the service and the UI.
      operation_name = 'fnapis%s' % transform.id

      def only_element(iterable):
        element, = iterable
        return element

      if transform.function_spec.urn == DATA_OUTPUT_URN:
        target = beam_fn_api_pb2.Target(
            primitive_transform_reference=transform.id,
            name=only_element(transform.outputs.keys()))

        op = DataOutputOperation(
            operation_name,
            transform.step_name,
            consumers[transform.id],
            counter_factory,
            state_sampler,
            coders[only_element(transform.outputs.values()).coder_reference],
            target,
            self.data_channel_factory.create_data_channel(
                transform.function_spec))

      elif transform.function_spec.urn == DATA_INPUT_URN:
        target = beam_fn_api_pb2.Target(
            primitive_transform_reference=transform.id,
            name=only_element(transform.inputs.keys()))
        op = DataInputOperation(
            operation_name,
            transform.step_name,
            consumers[transform.id],
            counter_factory,
            state_sampler,
            coders[only_element(transform.outputs.values()).coder_reference],
            target,
            self.data_channel_factory.create_data_channel(
                transform.function_spec))

      elif transform.function_spec.urn == PYTHON_DOFN_URN:
        def create_side_input(tag, si):
          # TODO(robertwb): Extract windows (and keys) out of element data.
          return operation_specs.WorkerSideInputSource(
              tag=tag,
              source=SideInputSource(
                  self.state_handler,
                  beam_fn_api_pb2.StateKey(
                      key=si.view_fn.id.encode('utf-8')),
                  coder=unpack_and_deserialize_py_fn(si.view_fn)))
        output_tags = list(transform.outputs.keys())
        spec = operation_specs.WorkerDoFn(
            serialized_fn=unpack_function_spec_data(transform.function_spec),
            output_tags=output_tags,
            input=None,
            side_inputs=[create_side_input(tag, si)
                         for tag, si in transform.side_inputs.items()],
            output_coders=[coders[transform.outputs[out].coder_reference]
                           for out in output_tags])

        op = operations.DoOperation(operation_name, spec, counter_factory,
                                    state_sampler)
        # TODO(robertwb): Move these to the constructor.
        op.step_name = transform.step_name
        for tag, op_consumers in consumers[transform.id].items():
          for consumer in op_consumers:
            op.add_receiver(
                consumer, output_tags.index(tag))

      elif transform.function_spec.urn == IDENTITY_DOFN_URN:
        op = operations.FlattenOperation(operation_name, None, counter_factory,
                                         state_sampler)
        # TODO(robertwb): Move these to the constructor.
        op.step_name = transform.step_name
        for tag, op_consumers in consumers[transform.id].items():
          for consumer in op_consumers:
            op.add_receiver(consumer, 0)

      elif transform.function_spec.urn == PYTHON_SOURCE_URN:
        source = load_compressed(unpack_function_spec_data(
            transform.function_spec))
        # TODO(vikasrk): Remove this once custom source is implemented with
        # splittable dofn via the data plane.
        spec = operation_specs.WorkerRead(
            iobase.SourceBundle(1.0, source, None, None),
            [WindowedValueCoder(source.default_output_coder())])
        op = operations.ReadOperation(operation_name, spec, counter_factory,
                                      state_sampler)
        op.step_name = transform.step_name
        output_tags = list(transform.outputs.keys())
        for tag, op_consumers in consumers[transform.id].items():
          for consumer in op_consumers:
            op.add_receiver(
                consumer, output_tags.index(tag))

      else:
        raise NotImplementedError

      # Record consumers.
      for _, inputs in transform.inputs.items():
        for target in inputs.target:
          consumers[target.primitive_transform_reference][target.name].append(
              op)

      reversed_ops.append(op)
      ops_by_id[transform.id] = op

    return list(reversed(reversed_ops)), ops_by_id

  def process_bundle(self, request, instruction_id):
    ops, ops_by_id = self.create_execution_tree(
        self.fns[request.process_bundle_descriptor_reference])

    expected_inputs = []
    for _, op in ops_by_id.items():
      if isinstance(op, DataOutputOperation):
        # TODO(robertwb): Is there a better way to pass the instruction id to
        # the operation?
        op.set_output_stream(op.data_channel.output_stream(
            instruction_id, op.target))
      elif isinstance(op, DataInputOperation):
        # We must wait until we receive "end of stream" for each of these ops.
        expected_inputs.append(op)

    # Start all operations.
    for op in reversed(ops):
      logging.info('start %s', op)
      op.start()

    # Inject inputs from data plane.
    for input_op in expected_inputs:
      for data in input_op.data_channel.input_elements(
          instruction_id, [input_op.target]):
        # ignores input name
        target_op = ops_by_id[data.target.primitive_transform_reference]
        # lacks coder for non-input ops
        target_op.process_encoded(data.data)

    # Finish all operations.
    for op in ops:
      logging.info('finish %s', op)
      op.finish()

    return beam_fn_api_pb2.ProcessBundleResponse()
