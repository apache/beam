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
import random
import re
from builtins import next
from builtins import object

from future.utils import itervalues

import apache_beam as beam
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders import coder_impl
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.dataflow import dataflow_runner
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.runners.worker import statesampler
from apache_beam.transforms import sideinputs
from apache_beam.transforms import userstate
from apache_beam.utils import counters
from apache_beam.utils import proto_utils

# This module is experimental. No backwards-compatibility guarantees.


DATA_INPUT_URN = 'urn:org.apache.beam:source:runner:0.1'
DATA_OUTPUT_URN = 'urn:org.apache.beam:sink:runner:0.1'
IDENTITY_DOFN_URN = 'urn:org.apache.beam:dofn:identity:0.1'
# TODO(vikasrk): Fix this once runner sends appropriate common_urns.
OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN = 'urn:beam:dofn:javasdk:0.1'
OLD_DATAFLOW_RUNNER_HARNESS_READ_URN = 'urn:org.apache.beam:source:java:0.1'


class RunnerIOOperation(operations.Operation):
  """Common baseclass for runner harness IO operations."""

  def __init__(self, name_context, step_name, consumers, counter_factory,
               state_sampler, windowed_coder, target, data_channel):
    super(RunnerIOOperation, self).__init__(
        name_context, None, counter_factory, state_sampler)
    self.windowed_coder = windowed_coder
    self.windowed_coder_impl = windowed_coder.get_impl()
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
    self.windowed_coder_impl.encode_to_stream(
        windowed_value, self.output_stream, True)
    self.output_stream.maybe_flush()

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
        operations.ConsumerSet(
            self.counter_factory, self.name_context.step_name, 0,
            next(iter(itervalues(consumers))), self.windowed_coder)]

  def process(self, windowed_value):
    self.output(windowed_value)

  def process_encoded(self, encoded_windowed_values):
    input_stream = coder_impl.create_InputStream(encoded_windowed_values)
    while input_stream.size() > 0:
      decoded_value = self.windowed_coder_impl.decode_from_stream(
          input_stream, True)
      self.output(decoded_value)


class _StateBackedIterable(object):
  def __init__(self, state_handler, state_key, coder):
    self._state_handler = state_handler
    self._state_key = state_key
    self._coder_impl = coder.get_impl()

  def __iter__(self):
    # TODO(robertwb): Support pagination.
    input_stream = coder_impl.create_InputStream(
        self._state_handler.blocking_get(self._state_key))
    while input_stream.size() > 0:
      yield self._coder_impl.decode_from_stream(input_stream, True)

  def __reduce__(self):
    return list, (list(self),)


class StateBackedSideInputMap(object):
  def __init__(self, state_handler, transform_id, tag, side_input_data, coder):
    self._state_handler = state_handler
    self._transform_id = transform_id
    self._tag = tag
    self._side_input_data = side_input_data
    self._element_coder = coder.wrapped_value_coder
    self._target_window_coder = coder.window_coder
    # TODO(robertwb): Limit the cache size.
    # TODO(robertwb): Cross-bundle caching respecting cache tokens.
    self._cache = {}

  def __getitem__(self, window):
    target_window = self._side_input_data.window_mapping_fn(window)
    if target_window not in self._cache:
      state_key = beam_fn_api_pb2.StateKey(
          multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
              ptransform_id=self._transform_id,
              side_input_id=self._tag,
              window=self._target_window_coder.encode(target_window),
              key=b''))
      state_handler = self._state_handler
      access_pattern = self._side_input_data.access_pattern

      if access_pattern == common_urns.side_inputs.ITERABLE.urn:
        raw_view = _StateBackedIterable(
            state_handler, state_key, self._element_coder)

      elif (access_pattern == common_urns.side_inputs.MULTIMAP.urn or
            access_pattern ==
            dataflow_runner._DataflowSideInput.DATAFLOW_MULTIMAP_URN):
        cache = {}
        key_coder_impl = self._element_coder.key_coder().get_impl()
        value_coder = self._element_coder.value_coder()

        class MultiMap(object):
          def __getitem__(self, key):
            if key not in cache:
              keyed_state_key = beam_fn_api_pb2.StateKey()
              keyed_state_key.CopyFrom(state_key)
              keyed_state_key.multimap_side_input.key = (
                  key_coder_impl.encode_nested(key))
              cache[key] = _StateBackedIterable(
                  state_handler, keyed_state_key, value_coder)
            return cache[key]

          def __reduce__(self):
            # TODO(robertwb): Figure out how to support this.
            raise TypeError(common_urns.side_inputs.MULTIMAP.urn)

        raw_view = MultiMap()

      else:
        raise ValueError(
            "Unknown access pattern: '%s'" % access_pattern)

      self._cache[target_window] = self._side_input_data.view_fn(raw_view)
    return self._cache[target_window]

  def is_globally_windowed(self):
    return (self._side_input_data.window_mapping_fn
            == sideinputs._global_window_mapping_fn)


class CombiningValueRuntimeState(userstate.RuntimeState):
  def __init__(self, underlying_bag_state, combinefn):
    self._combinefn = combinefn
    self._underlying_bag_state = underlying_bag_state

  def _read_accumulator(self, rewrite=True):
    merged_accumulator = self._combinefn.merge_accumulators(
        self._underlying_bag_state.read())
    if rewrite:
      self._underlying_bag_state.clear()
      self._underlying_bag_state.add(merged_accumulator)
    return merged_accumulator

  def read(self):
    return self._combinefn.extract_output(self._read_accumulator())

  def add(self, value):
    # Prefer blind writes, but don't let them grow unboundedly.
    # This should be tuned to be much lower, but for now exercise
    # both paths well.
    if random.random() < 0.5:
      accumulator = self._read_accumulator(False)
      self._underlying_bag_state.clear()
    else:
      accumulator = self._combinefn.create_accumulator()
    self._underlying_bag_state.add(
        self._combinefn.add_input(accumulator, value))

  def clear(self):
    self._underlying_bag_state.clear()


# TODO(BEAM-5428): Implement cross-bundle state caching.
class SynchronousBagRuntimeState(userstate.RuntimeState):
  def __init__(self, state_handler, state_key, value_coder):
    self._state_handler = state_handler
    self._state_key = state_key
    self._value_coder = value_coder

  def read(self):
    return _StateBackedIterable(
        self._state_handler, self._state_key, self._value_coder)

  def add(self, value):
    self._state_handler.blocking_append(
        self._state_key, self._value_coder.encode(value))

  def clear(self):
    self._state_handler.blocking_clear(self._state_key)


class FnApiUserStateContext(userstate.UserStateContext):
  def __init__(self, state_handler, transform_id, key_coder, window_coder):
    self._state_handler = state_handler
    self._transform_id = transform_id
    self._key_coder = key_coder
    self._window_coder = window_coder

  def get_timer(self, timer_spec, key, window):
    raise NotImplementedError

  def get_state(self, state_spec, key, window):
    if isinstance(state_spec,
                  (userstate.BagStateSpec, userstate.CombiningValueStateSpec)):
      bag_state = SynchronousBagRuntimeState(
          self._state_handler,
          state_key=beam_fn_api_pb2.StateKey(
              bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
                  ptransform_id=self._transform_id,
                  user_state_id=state_spec.name,
                  window=self._window_coder.encode(window),
                  key=self._key_coder.encode(key))),
          value_coder=state_spec.coder)
      if isinstance(state_spec, userstate.BagStateSpec):
        return bag_state
      else:
        return CombiningValueRuntimeState(bag_state, state_spec.combine_fn)
    else:
      raise NotImplementedError(state_spec)


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
  """A class for processing bundles of elements."""
  def __init__(
      self, process_bundle_descriptor, state_handler, data_channel_factory):
    self.process_bundle_descriptor = process_bundle_descriptor
    self.state_handler = state_handler
    self.data_channel_factory = data_channel_factory
    # TODO(robertwb): Figure out the correct prefix to use for output counters
    # from StateSampler.
    self.counter_factory = counters.CounterFactory()
    self.state_sampler = statesampler.StateSampler(
        'fnapi-step-%s' % self.process_bundle_descriptor.id,
        self.counter_factory)
    self.ops = self.create_execution_tree(self.process_bundle_descriptor)

  def create_execution_tree(self, descriptor):

    transform_factory = BeamTransformFactory(
        descriptor, self.data_channel_factory, self.counter_factory,
        self.state_sampler, self.state_handler)

    def is_side_input(transform_proto, tag):
      if transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
        return tag in proto_utils.parse_Bytes(
            transform_proto.spec.payload,
            beam_runner_api_pb2.ParDoPayload).side_inputs

    pcoll_consumers = collections.defaultdict(list)
    for transform_id, transform_proto in descriptor.transforms.items():
      for tag, pcoll_id in transform_proto.inputs.items():
        if not is_side_input(transform_proto, tag):
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

    return collections.OrderedDict([
        (transform_id, get_operation(transform_id))
        for transform_id in sorted(
            descriptor.transforms, key=topological_height, reverse=True)])

  def process_bundle(self, instruction_id):

    expected_inputs = []
    for op in self.ops.values():
      if isinstance(op, DataOutputOperation):
        # TODO(robertwb): Is there a better way to pass the instruction id to
        # the operation?
        op.set_output_stream(op.data_channel.output_stream(
            instruction_id, op.target))
      elif isinstance(op, DataInputOperation):
        # We must wait until we receive "end of stream" for each of these ops.
        expected_inputs.append(op)

    try:
      self.state_sampler.start()
      # Start all operations.
      for op in reversed(self.ops.values()):
        logging.debug('start %s', op)
        op.start()

      # Inject inputs from data plane.
      for input_op in expected_inputs:
        for data in input_op.data_channel.input_elements(
            instruction_id, [input_op.target]):
          # ignores input name
          input_op.process_encoded(data.data)

      # Finish all operations.
      for op in self.ops.values():
        logging.debug('finish %s', op)
        op.finish()
    finally:
      self.state_sampler.stop_if_still_running()

  def metrics(self):
    return beam_fn_api_pb2.Metrics(
        # TODO(robertwb): Rename to progress?
        ptransforms={
            transform_id:
            self._fix_output_tags(transform_id, op.progress_metrics())
            for transform_id, op in self.ops.items()})

  def _fix_output_tags(self, transform_id, metrics):
    # Outputs are still referred to by index, not by name, in many Operations.
    # However, if there is exactly one output, we can fix up the name here.
    def fix_only_output_tag(actual_output_tag, mapping):
      if len(mapping) == 1:
        fake_output_tag, count = only_element(list(mapping.items()))
        if fake_output_tag != actual_output_tag:
          del mapping[fake_output_tag]
          mapping[actual_output_tag] = count
    actual_output_tags = list(
        self.process_bundle_descriptor.transforms[transform_id].outputs.keys())
    if len(actual_output_tags) == 1:
      fix_only_output_tag(
          actual_output_tags[0],
          metrics.processed_elements.measured.output_element_counts)
      fix_only_output_tag(
          actual_output_tags[0],
          metrics.active_elements.measured.output_element_counts)
    return metrics


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
    if coder_id not in self.descriptor.coders:
      raise KeyError("No such coder: %s" % coder_id)
    coder_proto = self.descriptor.coders[coder_id]
    if coder_proto.spec.spec.urn:
      return self.context.coders.get_by_id(coder_id)
    else:
      # No URN, assume cloud object encoding json bytes.
      return operation_specs.get_coder_from_spec(
          json.loads(coder_proto.spec.spec.payload))

  def get_windowed_coder(self, pcoll_id):
    coder = self.get_coder(self.descriptor.pcollections[pcoll_id].coder_id)
    # TODO(robertwb): Remove this condition once all runners are consistent.
    if not isinstance(coder, WindowedValueCoder):
      windowing_strategy = self.descriptor.windowing_strategies[
          self.descriptor.pcollections[pcoll_id].windowing_strategy_id]
      return WindowedValueCoder(
          coder, self.get_coder(windowing_strategy.window_coder_id))
    else:
      return coder

  def get_output_coders(self, transform_proto):
    return {
        tag: self.get_windowed_coder(pcoll_id)
        for tag, pcoll_id in transform_proto.outputs.items()
    }

  def get_only_output_coder(self, transform_proto):
    return only_element(self.get_output_coders(transform_proto).values())

  def get_input_coders(self, transform_proto):
    return {
        tag: self.get_windowed_coder(pcoll_id)
        for tag, pcoll_id in transform_proto.inputs.items()
    }

  def get_only_input_coder(self, transform_proto):
    return only_element(list(self.get_input_coders(transform_proto).values()))

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
      name=only_element(list(transform_proto.outputs.keys())))
  return DataInputOperation(
      transform_proto.unique_name,
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      factory.get_coder(grpc_port.coder_id)
      if grpc_port.coder_id
      else factory.get_only_output_coder(transform_proto),
      input_target=target,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(
    DATA_OUTPUT_URN, beam_fn_api_pb2.RemoteGrpcPort)
def create(factory, transform_id, transform_proto, grpc_port, consumers):
  target = beam_fn_api_pb2.Target(
      primitive_transform_reference=transform_id,
      name=only_element(list(transform_proto.inputs.keys())))
  return DataOutputOperation(
      transform_proto.unique_name,
      transform_proto.unique_name,
      consumers,
      factory.counter_factory,
      factory.state_sampler,
      factory.get_coder(grpc_port.coder_id)
      if grpc_port.coder_id
      else factory.get_only_input_coder(transform_proto),
      target=target,
      data_channel=factory.data_channel_factory.create_data_channel(grpc_port))


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_READ_URN, None)
def create(factory, transform_id, transform_proto, parameter, consumers):
  # The Dataflow runner harness strips the base64 encoding.
  source = pickler.loads(base64.b64encode(parameter))
  spec = operation_specs.WorkerRead(
      iobase.SourceBundle(1.0, source, None, None),
      [factory.get_only_output_coder(transform_proto)])
  return factory.augment_oldstyle_op(
      operations.ReadOperation(
          transform_proto.unique_name,
          spec,
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.deprecated_primitives.READ.urn, beam_runner_api_pb2.ReadPayload)
def create(factory, transform_id, transform_proto, parameter, consumers):
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


@BeamTransformFactory.register_urn(
    python_urns.IMPULSE_READ_TRANSFORM, beam_runner_api_pb2.ReadPayload)
def create(factory, transform_id, transform_proto, parameter, consumers):
  return operations.ImpulseReadOperation(
      transform_proto.unique_name,
      factory.counter_factory,
      factory.state_sampler,
      consumers,
      iobase.SourceBase.from_runner_api(
          parameter.source, factory.context),
      factory.get_only_output_coder(transform_proto))


@BeamTransformFactory.register_urn(OLD_DATAFLOW_RUNNER_HARNESS_PARDO_URN, None)
def create(factory, transform_id, transform_proto, serialized_fn, consumers):
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers, serialized_fn)


@BeamTransformFactory.register_urn(
    common_urns.primitives.PAR_DO.urn, beam_runner_api_pb2.ParDoPayload)
def create(factory, transform_id, transform_proto, parameter, consumers):
  assert parameter.do_fn.spec.urn == python_urns.PICKLED_DOFN_INFO
  serialized_fn = parameter.do_fn.spec.payload
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      serialized_fn, parameter.side_inputs)


def _create_pardo_operation(
    factory, transform_id, transform_proto, consumers,
    serialized_fn, side_inputs_proto=None):

  if side_inputs_proto:
    input_tags_to_coders = factory.get_input_coders(transform_proto)
    tagged_side_inputs = [
        (tag, beam.pvalue.SideInputData.from_runner_api(si, factory.context))
        for tag, si in side_inputs_proto.items()]
    tagged_side_inputs.sort(
        key=lambda tag_si: int(re.match('side([0-9]+)(-.*)?$',
                                        tag_si[0]).group(1)))
    side_input_maps = [
        StateBackedSideInputMap(
            factory.state_handler,
            transform_id,
            tag,
            si,
            input_tags_to_coders[tag])
        for tag, si in tagged_side_inputs]
  else:
    side_input_maps = []

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
    side_input_tags = side_inputs_proto or ()
    pcoll_id, = [pcoll for tag, pcoll in transform_proto.inputs.items()
                 if tag not in side_input_tags]
    windowing = factory.context.windowing_strategies.get_by_id(
        factory.descriptor.pcollections[pcoll_id].windowing_strategy_id)
    serialized_fn = pickler.dumps(dofn_data[:-1] + (windowing,))

  if userstate.is_stateful_dofn(dofn_data[0]):
    input_coder = factory.get_only_input_coder(transform_proto)
    user_state_context = FnApiUserStateContext(
        factory.state_handler,
        transform_id,
        input_coder.key_coder(),
        input_coder.window_coder)
  else:
    user_state_context = None

  output_coders = factory.get_output_coders(transform_proto)
  spec = operation_specs.WorkerDoFn(
      serialized_fn=serialized_fn,
      output_tags=[mutate_tag(tag) for tag in output_tags],
      input=None,
      side_inputs=None,  # Fn API uses proto definitions and the Fn State API
      output_coders=[output_coders[tag] for tag in output_tags])

  return factory.augment_oldstyle_op(
      operations.DoOperation(
          transform_proto.unique_name,
          spec,
          factory.counter_factory,
          factory.state_sampler,
          side_input_maps,
          user_state_context),
      transform_proto.unique_name,
      consumers,
      output_tags)


def _create_simple_pardo_operation(
    factory, transform_id, transform_proto, consumers, dofn):
  serialized_fn = pickler.dumps((dofn, (), {}, [], None))
  return _create_pardo_operation(
      factory, transform_id, transform_proto, consumers, serialized_fn)


@BeamTransformFactory.register_urn(
    common_urns.primitives.ASSIGN_WINDOWS.urn,
    beam_runner_api_pb2.WindowingStrategy)
def create(factory, transform_id, transform_proto, parameter, consumers):
  class WindowIntoDoFn(beam.DoFn):
    def __init__(self, windowing):
      self.windowing = windowing

    def process(self, element, timestamp=beam.DoFn.TimestampParam,
                window=beam.DoFn.WindowParam):
      new_windows = self.windowing.windowfn.assign(
          WindowFn.AssignContext(timestamp, element=element, window=window))
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


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PGBKCV.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  # TODO: Combine side inputs.
  serialized_combine_fn = pickler.dumps(
      (beam.CombineFn.from_runner_api(payload.combine_fn, factory.context),
       [], {}))
  return factory.augment_oldstyle_op(
      operations.PGBKCVOperation(
          transform_proto.unique_name,
          operation_specs.WorkerPartialGroupByKey(
              serialized_combine_fn,
              None,
              [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_MERGE_ACCUMULATORS.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  return _create_combine_phase_operation(
      factory, transform_proto, payload, consumers, 'merge')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_EXTRACT_OUTPUTS.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  return _create_combine_phase_operation(
      factory, transform_proto, payload, consumers, 'extract')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_PRECOMBINE.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  serialized_combine_fn = pickler.dumps(
      (beam.CombineFn.from_runner_api(payload.combine_fn, factory.context),
       [], {}))
  return factory.augment_oldstyle_op(
      operations.PGBKCVOperation(
          transform_proto.unique_name,
          operation_specs.WorkerPartialGroupByKey(
              serialized_combine_fn,
              None,
              [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_MERGE_ACCUMULATORS.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  return _create_combine_phase_operation(
      factory, transform_proto, payload, consumers, 'merge')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_PER_KEY_EXTRACT_OUTPUTS.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  return _create_combine_phase_operation(
      factory, transform_proto, payload, consumers, 'extract')


@BeamTransformFactory.register_urn(
    common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
    beam_runner_api_pb2.CombinePayload)
def create(factory, transform_id, transform_proto, payload, consumers):
  return _create_combine_phase_operation(
      factory, transform_proto, payload, consumers, 'all')


def _create_combine_phase_operation(
    factory, transform_proto, payload, consumers, phase):
  serialized_combine_fn = pickler.dumps(
      (beam.CombineFn.from_runner_api(payload.combine_fn, factory.context),
       [], {}))
  return factory.augment_oldstyle_op(
      operations.CombineOperation(
          transform_proto.unique_name,
          operation_specs.WorkerCombineFn(
              serialized_combine_fn,
              phase,
              None,
              [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(common_urns.primitives.FLATTEN.urn, None)
def create(factory, transform_id, transform_proto, unused_parameter, consumers):
  return factory.augment_oldstyle_op(
      operations.FlattenOperation(
          transform_proto.unique_name,
          operation_specs.WorkerFlatten(
              None,
              [factory.get_only_output_coder(transform_proto)]),
          factory.counter_factory,
          factory.state_sampler),
      transform_proto.unique_name,
      consumers)


@BeamTransformFactory.register_urn(
    common_urns.primitives.MAP_WINDOWS.urn,
    beam_runner_api_pb2.SdkFunctionSpec)
def create(factory, transform_id, transform_proto, mapping_fn_spec, consumers):
  assert mapping_fn_spec.spec.urn == python_urns.PICKLED_WINDOW_MAPPING_FN
  window_mapping_fn = pickler.loads(mapping_fn_spec.spec.payload)

  class MapWindows(beam.DoFn):

    def process(self, element):
      key, window = element
      return [(key, window_mapping_fn(window))]

  return _create_simple_pardo_operation(
      factory, transform_id, transform_proto, consumers,
      MapWindows())
