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

import base64
import collections
import json
import logging

from google.protobuf import wrappers_pb2

import apache_beam as beam
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders import coder_impl
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.dataflow.native_io import iobase as native_iobase
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.utils import counters
from apache_beam.utils import proto_utils
from apache_beam.utils import urns

# This module is experimental. No backwards-compatibility guarantees.


try:
  from apache_beam.runners.worker import statesampler
except ImportError:
  from apache_beam.runners.worker import statesampler_fake as statesampler


DATA_INPUT_URN = 'urn:org.apache.beam:source:runner:0.1'
DATA_OUTPUT_URN = 'urn:org.apache.beam:sink:runner:0.1'
IDENTITY_DOFN_URN = 'urn:org.apache.beam:dofn:identity:0.1'
PYTHON_ITERABLE_VIEWFN_URN = 'urn:org.apache.beam:viewfn:iterable:python:0.1'
PYTHON_CODER_URN = 'urn:org.apache.beam:coder:python:0.1'
# TODO(vikasrk): Fix this once runner sends appropriate python urns.
OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN = 'urn:beam:dofn:javasdk:0.1'
OLD_DATAFLOW_RUNNER_HARNESS_READ_URN = 'urn:org.apache.beam:source:java:0.1'


def side_input_tag(transform_id, tag):
  return str("%d[%s][%s]" % (len(transform_id), transform_id, tag))


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


def memoize(func):
  cache = {}
  missing = object()

  def wrapper(*args):
    result = cache.get(args, missing)
    if result is missing:
      result = cache[args] = func(*args)
    return result
  return wrapper


def only_element(iterable):
  element, = iterable
  return element


class BundleProcessor(object):
  """A class for processing bundles of elements.
  """
  def __init__(
      self, process_bundle_descriptor, state_handler, data_channel_factory):
    self.process_bundle_descriptor = process_bundle_descriptor
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory

  def create_execution_tree(self, descriptor):
    # TODO(robertwb): Figure out the correct prefix to use for output counters
    # from StateSampler.
    counter_factory = counters.CounterFactory()
    state_sampler = statesampler.StateSampler(
        'fnapi-step%s' % descriptor.id, counter_factory)

    transform_factory = BeamTransformFactory(
        descriptor, self.data_channel_factory, counter_factory, state_sampler,
        self.state_handler)

    pcoll_consumers = collections.defaultdict(list)
    for transform_id, transform_proto in descriptor.transforms.items():
      for pcoll_id in transform_proto.inputs.values():
        pcoll_consumers[pcoll_id].append(transform_id)

    @memoize
    def get_operation(transform_id):
      transform_consumers = {
          tag: [get_operation(op) for op in pcoll_consumers[pcoll_id]]
          for tag, pcoll_id
          in descriptor.transforms[transform_id].outputs.items()
      }
      return transform_factory.create_operation(
          transform_id, transform_consumers)

    # Operations must be started (hence returned) in order.
    @memoize
    def topological_height(transform_id):
      return 1 + max(
          [0] +
          [topological_height(consumer)
           for pcoll in descriptor.transforms[transform_id].outputs.values()
           for consumer in pcoll_consumers[pcoll]])

    return [get_operation(transform_id)
            for transform_id in sorted(
                descriptor.transforms, key=topological_height, reverse=True)]

  def process_bundle(self, instruction_id):
    ops = self.create_execution_tree(self.process_bundle_descriptor)

    expected_inputs = []
    for op in ops:
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
        input_op.process_encoded(data.data)

    # Finish all operations.
    for op in ops:
      logging.info('finish %s', op)
      op.finish()


class BeamTransformFactory(object):
  """Factory for turning transform_protos into executable operations."""
  def __init__(self, descriptor, data_channel_factory, counter_factory,
               state_sampler, state_handler):
    self.descriptor = descriptor
    self.data_channel_factory = data_channel_factory
    self.counter_factory = counter_factory
    self.state_sampler = state_sampler
    self.state_handler = state_handler
    self.context = pipeline_context.PipelineContext(descriptor)

  _known_urns = {}

  @classmethod
  def register_urn(cls, urn, parameter_type):
    def wrapper(func):
      cls._known_urns[urn] = func, parameter_type
      return func
    return wrapper

  def create_operation(self, transform_id, consumers):
    transform_proto = self.descriptor.transforms[transform_id]
    creator, parameter_type = self._known_urns[transform_proto.spec.urn]
    payload = proto_utils.parse_Bytes(
        transform_proto.spec.payload, parameter_type)
    return creator(self, transform_id, transform_proto, payload, consumers)

  def get_coder(self, coder_id):
    coder_proto = self.descriptor.coders[coder_id]
    if coder_proto.spec.spec.urn:
      return self.context.coders.get_by_id(coder_id)
    else:
      # No URN, assume cloud object encoding json bytes.
      return operation_specs.get_coder_from_spec(
          json.loads(coder_proto.spec.spec.payload))

  def get_output_coders(self, transform_proto):
    return {
        tag: self.get_coder(self.descriptor.pcollections[pcoll_id].coder_id)
        for tag, pcoll_id in transform_proto.outputs.items()
    }

  def get_only_output_coder(self, transform_proto):
    return only_element(self.get_output_coders(transform_proto).values())

  def get_input_coders(self, transform_proto):
    return {
        tag: self.get_coder(self.descriptor.pcollections[pcoll_id].coder_id)
        for tag, pcoll_id in transform_proto.inputs.items()
    }

  def get_only_input_coder(self, transform_proto):
    return only_element(self.get_input_coders(transform_proto).values())

  # TODO(robertwb): Update all operations to take these in the constructor.
  @staticmethod
  def augment_oldstyle_op(op, step_name, consumers, tag_list=None):
    op.step_name = step_name
    for tag, op_consumers in consumers.items():
      for consumer in op_consumers:
        op.add_receiver(consumer, tag_list.index(tag) if tag_list else 0)
    return op


@BeamTransformFactory.register_urn(
    DATA_INPUT_URN, beam_fn_api_pb2.RemoteGrpcPort)
def create(factory, transform_id, transform_proto, grpc_port, consumers):
  target = beam_fn_api_pb2.Target(
      primitive_transform_reference=transform_id,
      name=only_element(transform_proto.outputs.keys()))
  return DataInputOperation(
      transform_proto.unique_name,
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      factory.get_only_output_coder(transform_proto),
      input_target=target,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(
    DATA_OUTPUT_URN, beam_fn_api_pb2.RemoteGrpcPort)
def create(factory, transform_id, transform_proto, grpc_port, consumers):
  target = beam_fn_api_pb2.Target(
      primitive_transform_reference=transform_id,
      name=only_element(transform_proto.inputs.keys()))
  return DataOutputOperation(
      transform_proto.unique_name,
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      # TODO(robertwb): Perhaps this could be distinct from the input coder?
      factory.get_only_input_coder(transform_proto),
      target=target,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_READ_URN, None)
def create(factory, transform_id, transform_proto, parameter, consumers):
  # The Dataflow runner harness strips the base64 encoding.
  source = pickler.loads(base64.b64encode(parameter))
  spec = operation_specs.WorkerRead(
      iobase.SourceBundle(1.0, source, None, None),
      [WindowedValueCoder(source.default_output_coder())])
  return factory.augment_oldstyle_op(
      operations.ReadOperation(
          transform_proto.unique_name,
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    urns.READ_TRANSFORM, beam_runner_api_pb2.ReadPayload)
def create(factory, transform_id, transform_proto, parameter, consumers):
  # The Dataflow runner harness strips the base64 encoding.
  source = iobase.SourceBase.from_runner_api(parameter.source, factory.context)
  spec = operation_specs.WorkerRead(
      iobase.SourceBundle(1.0, source, None, None),
      [WindowedValueCoder(source.default_output_coder())])
  return factory.augment_oldstyle_op(
      operations.ReadOperation(
          transform_proto.unique_name,
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN, None)
def create(factory, transform_id, transform_proto, parameter, consumers):
  dofn_data = pickler.loads(parameter)
  if len(dofn_data) == 2:
    # Has side input data.
    serialized_fn, side_input_data = dofn_data
  else:
    # No side input data.
    serialized_fn, side_input_data = parameter, []
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      serialized_fn, side_input_data)


@BeamTransformFactory.register_urn(
    urns.PARDO_TRANSFORM, beam_runner_api_pb2.ParDoPayload)
def create(factory, transform_id, transform_proto, parameter, consumers):
  assert parameter.do_fn.spec.urn == urns.PICKLED_DO_FN_INFO
  serialized_fn = parameter.do_fn.spec.payload
  dofn_data = pickler.loads(serialized_fn)
  if len(dofn_data) == 2:
    # Has side input data.
    serialized_fn, side_input_data = dofn_data
  else:
    # No side input data.
    side_input_data = []
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      serialized_fn, side_input_data)


def _create_pardo_operation(
    factory, transform_id, transform_proto, consumers,
    serialized_fn, side_input_data):
  def create_side_input(tag, coder):
    # TODO(robertwb): Extract windows (and keys) out of element data.
    # TODO(robertwb): Extract state key from ParDoPayload.
    return operation_specs.WorkerSideInputSource(
        tag=tag,
        source=SideInputSource(
            factory.state_handler,
            beam_fn_api_pb2.StateKey.MultimapSideInput(
                key=side_input_tag(transform_id, tag)),
            coder=coder))
  output_tags = list(transform_proto.outputs.keys())

  # Hack to match out prefix injected by dataflow runner.
  def mutate_tag(tag):
    if 'None' in output_tags:
      if tag == 'None':
        return 'out'
      else:
        return 'out_' + tag
    else:
      return tag
  dofn_data = pickler.loads(serialized_fn)
  if not dofn_data[-1]:
    # Windowing not set.
    pcoll_id, = transform_proto.inputs.values()
    windowing = factory.context.windowing_strategies.get_by_id(
        factory.descriptor.pcollections[pcoll_id].windowing_strategy_id)
    serialized_fn = pickler.dumps(dofn_data[:-1] + (windowing,))
  output_coders = factory.get_output_coders(transform_proto)
  spec = operation_specs.WorkerDoFn(
      serialized_fn=serialized_fn,
      output_tags=[mutate_tag(tag) for tag in output_tags],
      input=None,
      side_inputs=[
          create_side_input(tag, coder) for tag, coder in side_input_data],
      output_coders=[output_coders[tag] for tag in output_tags])
  return factory.augment_oldstyle_op(
      operations.DoOperation(
          transform_proto.unique_name,
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers,
      output_tags)


def _create_simple_pardo_operation(
    factory, transform_id, transform_proto, consumers, dofn):
  serialized_fn = pickler.dumps((dofn, (), {}, [], None))
  side_input_data = []
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      serialized_fn, side_input_data)


@BeamTransformFactory.register_urn(
    urns.GROUP_ALSO_BY_WINDOW_TRANSFORM, wrappers_pb2.BytesValue)
def create(factory, transform_id, transform_proto, parameter, consumers):
  # Perhaps this hack can go away once all apply overloads are gone.
  from apache_beam.transforms.core import _GroupAlsoByWindowDoFn
  return _create_simple_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      _GroupAlsoByWindowDoFn(
          factory.context.windowing_strategies.get_by_id(parameter.value)))


@BeamTransformFactory.register_urn(
    urns.WINDOW_INTO_TRANSFORM, beam_runner_api_pb2.WindowingStrategy)
def create(factory, transform_id, transform_proto, parameter, consumers):
  class WindowIntoDoFn(beam.DoFn):
    def __init__(self, windowing):
      self.windowing = windowing

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
      new_windows = self.windowing.windowfn.assign(
          WindowFn.AssignContext(timestamp, element=element))
      yield WindowedValue(element, timestamp, new_windows)
  from apache_beam.transforms.core import Windowing
  from apache_beam.transforms.window import WindowFn, WindowedValue
  windowing = Windowing.from_runner_api(parameter, factory.context)
  return _create_simple_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      WindowIntoDoFn(windowing))


@BeamTransformFactory.register_urn(IDENTITY_DOFN_URN, None)
def create(factory, transform_id, transform_proto, unused_parameter, consumers):
  return factory.augment_oldstyle_op(
      operations.FlattenOperation(
          transform_proto.unique_name,
          operation_specs.WorkerFlatten(
              None, [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)
