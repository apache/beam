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
import copy
import logging
import Queue as queue
import re
import threading
import time
from concurrent import futures

import grpc

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam import metrics
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders import registry
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.internal import pickler
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners import runner
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import sdk_worker
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import proto_utils

# This module is experimental. No backwards-compatibility guarantees.


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):

  _DONE = object()

  def __init__(self):
    self._push_queue = queue.Queue()
    self._futures_by_id = dict()
    self._read_thread = threading.Thread(
        name='beam_control_read', target=self._read)
    self._started = False
    self._uid_counter = 0

  def Control(self, iterator, context):
    self._inputs = iterator
    # Note: We only support one client for now.
    self._read_thread.start()
    self._started = True
    while True:
      to_push = self._push_queue.get()
      if to_push is self._DONE:
        return
      yield to_push

  def _read(self):
    for data in self._inputs:
      self._futures_by_id.pop(data.instruction_id).set(data)

  def push(self, item):
    if item is self._DONE:
      future = None
    else:
      if not item.instruction_id:
        self._uid_counter += 1
        item.instruction_id = 'control_%s' % self._uid_counter
      future = ControlFuture(item.instruction_id)
      self._futures_by_id[item.instruction_id] = future
    self._push_queue.put(item)
    return future

  def done(self):
    self.push(self._DONE)
    # Can't join a thread before it's started.
    while not self._started:
      time.sleep(.01)
    self._read_thread.join()


class _GroupingBuffer(object):
  """Used to accumulate groupded (shuffled) results."""
  def __init__(self, pre_grouped_coder, post_grouped_coder, windowing):
    self._key_coder = pre_grouped_coder.key_coder()
    self._pre_grouped_coder = pre_grouped_coder
    self._post_grouped_coder = post_grouped_coder
    self._table = collections.defaultdict(list)
    self._windowing = windowing

  def append(self, elements_data):
    input_stream = create_InputStream(elements_data)
    coder_impl = self._pre_grouped_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    # TODO(robertwb): We could optimize this even more by using a
    # window-dropping coder for the data plane.
    is_trivial_windowing = self._windowing.is_default()
    while input_stream.size() > 0:
      windowed_key_value = coder_impl.decode_from_stream(input_stream, True)
      key, value = windowed_key_value.value
      self._table[key_coder_impl.encode(key)].append(
          value if is_trivial_windowing
          else windowed_key_value.with_value(value))

  def __iter__(self):
    output_stream = create_OutputStream()
    if self._windowing.is_default():
      globally_window = GlobalWindows.windowed_value(None).with_value
      windowed_key_values = lambda key, values: [globally_window((key, values))]
    else:
      trigger_driver = trigger.create_trigger_driver(self._windowing, True)
      windowed_key_values = trigger_driver.process_entire_key
    coder_impl = self._post_grouped_coder.get_impl()
    key_coder_impl = self._key_coder.get_impl()
    for encoded_key, windowed_values in self._table.items():
      key = key_coder_impl.decode(encoded_key)
      for wkvs in windowed_key_values(key, windowed_values):
        coder_impl.encode_to_stream(wkvs, output_stream, True)
    return iter([output_stream.get()])


class _WindowGroupingBuffer(object):
  """Used to partition windowed side inputs."""
  def __init__(self, side_input_data):
    # Here's where we would use a different type of partitioning
    # (e.g. also by key) for a different access pattern.
    assert side_input_data.access_pattern == common_urns.ITERABLE_SIDE_INPUT
    self._windowed_value_coder = side_input_data.coder
    self._window_coder = side_input_data.coder.window_coder
    self._value_coder = side_input_data.coder.wrapped_value_coder
    self._values_by_window = collections.defaultdict(list)

  def append(self, elements_data):
    input_stream = create_InputStream(elements_data)
    while input_stream.size() > 0:
      windowed_value = self._windowed_value_coder.get_impl(
          ).decode_from_stream(input_stream, True)
      for window in windowed_value.windows:
        self._values_by_window[window].append(windowed_value.value)

  def items(self):
    value_coder_impl = self._value_coder.get_impl()
    for window, values in self._values_by_window.items():
      encoded_window = self._window_coder.encode(window)
      output_stream = create_OutputStream()
      for value in values:
        value_coder_impl.encode_to_stream(value, output_stream, True)
      yield encoded_window, output_stream.get()


class FnApiRunner(runner.PipelineRunner):

  def __init__(self, use_grpc=False, sdk_harness_factory=None):
    """Creates a new Fn API Runner.

    Args:
      use_grpc: whether to use grpc or simply make in-process calls
          defaults to False
      sdk_harness_factory: callable used to instantiate customized sdk harnesses
          typcially not set by users
    """
    super(FnApiRunner, self).__init__()
    self._last_uid = -1
    self._use_grpc = use_grpc
    if sdk_harness_factory and not use_grpc:
      raise ValueError('GRPC must be used if a harness factory is provided.')
    self._sdk_harness_factory = sdk_harness_factory
    self._progress_frequency = None

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def run_pipeline(self, pipeline):
    MetricsEnvironment.set_metrics_supported(False)
    # This is sometimes needed if type checking is disabled
    # to enforce that the inputs (and outputs) of GroupByKey operations
    # are known to be KVs.
    from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
    pipeline.visit(DataflowRunner.group_by_key_input_visitor())
    return self.run_via_runner_api(pipeline.to_runner_api())

  def run_via_runner_api(self, pipeline_proto):
    return self.run_stages(*self.create_stages(pipeline_proto))

  def create_stages(self, pipeline_proto):

    # First define a couple of helpers.

    def union(a, b):
      # Minimize the number of distinct sets.
      if not a or a == b:
        return b
      elif not b:
        return a
      else:
        return frozenset.union(a, b)

    class Stage(object):
      """A set of Transforms that can be sent to the worker for processing."""
      def __init__(self, name, transforms,
                   downstream_side_inputs=None, must_follow=frozenset()):
        self.name = name
        self.transforms = transforms
        self.downstream_side_inputs = downstream_side_inputs
        self.must_follow = must_follow

      def __repr__(self):
        must_follow = ', '.join(prev.name for prev in self.must_follow)
        downstream_side_inputs = ', '.join(
            str(si) for si in self.downstream_side_inputs)
        return "%s\n  %s\n  must follow: %s\n  downstream_side_inputs: %s" % (
            self.name,
            '\n'.join(["%s:%s" % (transform.unique_name, transform.spec.urn)
                       for transform in self.transforms]),
            must_follow,
            downstream_side_inputs)

      def can_fuse(self, consumer):
        def no_overlap(a, b):
          return not a.intersection(b)
        return (
            not self in consumer.must_follow
            and not self.is_flatten() and not consumer.is_flatten()
            and no_overlap(self.downstream_side_inputs, consumer.side_inputs()))

      def fuse(self, other):
        return Stage(
            "(%s)+(%s)" % (self.name, other.name),
            self.transforms + other.transforms,
            union(self.downstream_side_inputs, other.downstream_side_inputs),
            union(self.must_follow, other.must_follow))

      def is_flatten(self):
        return any(transform.spec.urn == common_urns.FLATTEN_TRANSFORM
                   for transform in self.transforms)

      def side_inputs(self):
        for transform in self.transforms:
          if transform.spec.urn == common_urns.PARDO_TRANSFORM:
            payload = proto_utils.parse_Bytes(
                transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
            for side_input in payload.side_inputs:
              yield transform.inputs[side_input]

      def has_as_main_input(self, pcoll):
        for transform in self.transforms:
          if transform.spec.urn == common_urns.PARDO_TRANSFORM:
            payload = proto_utils.parse_Bytes(
                transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
            local_side_inputs = payload.side_inputs
          else:
            local_side_inputs = {}
          for local_id, pipeline_id in transform.inputs.items():
            if pcoll == pipeline_id and local_id not in local_side_inputs:
              return True

      def deduplicate_read(self):
        seen_pcolls = set()
        new_transforms = []
        for transform in self.transforms:
          if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
            pcoll = only_element(transform.outputs.items())[1]
            if pcoll in seen_pcolls:
              continue
            seen_pcolls.add(pcoll)
          new_transforms.append(transform)
        self.transforms = new_transforms

    # Now define the "optimization" phases.

    safe_coders = {}

    def lift_combiners(stages):
      """Expands CombinePerKey into pre- and post-grouping stages.

      ... -> CombinePerKey -> ...

      becomes

      ... -> PreCombine -> GBK -> MergeAccumulators -> ExtractOutput -> ...
      """
      def add_or_get_coder_id(coder_proto):
        for coder_id, coder in pipeline_components.coders.items():
          if coder == coder_proto:
            return coder_id
        new_coder_id = unique_name(pipeline_components.coders, 'coder')
        pipeline_components.coders[new_coder_id].CopyFrom(coder_proto)
        return new_coder_id

      def windowed_coder_id(coder_id):
        proto = beam_runner_api_pb2.Coder(
            spec=beam_runner_api_pb2.SdkFunctionSpec(
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=common_urns.WINDOWED_VALUE_CODER)),
            component_coder_ids=[coder_id, window_coder_id])
        return add_or_get_coder_id(proto)

      for stage in stages:
        assert len(stage.transforms) == 1
        transform = stage.transforms[0]
        if transform.spec.urn == common_urns.COMBINE_PER_KEY_TRANSFORM:
          combine_payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.CombinePayload)

          input_pcoll = pipeline_components.pcollections[only_element(
              transform.inputs.values())]
          output_pcoll = pipeline_components.pcollections[only_element(
              transform.outputs.values())]

          windowed_input_coder = pipeline_components.coders[
              input_pcoll.coder_id]
          element_coder_id, window_coder_id = (
              windowed_input_coder.component_coder_ids)
          element_coder = pipeline_components.coders[element_coder_id]
          key_coder_id, _ = element_coder.component_coder_ids
          accumulator_coder_id = combine_payload.accumulator_coder_id

          key_accumulator_coder = beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.SdkFunctionSpec(
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.KV_CODER)),
              component_coder_ids=[key_coder_id, accumulator_coder_id])
          key_accumulator_coder_id = add_or_get_coder_id(key_accumulator_coder)

          accumulator_iter_coder = beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.SdkFunctionSpec(
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.ITERABLE_CODER)),
              component_coder_ids=[accumulator_coder_id])
          accumulator_iter_coder_id = add_or_get_coder_id(
              accumulator_iter_coder)

          key_accumulator_iter_coder = beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.SdkFunctionSpec(
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.KV_CODER)),
              component_coder_ids=[key_coder_id, accumulator_iter_coder_id])
          key_accumulator_iter_coder_id = add_or_get_coder_id(
              key_accumulator_iter_coder)

          precombined_pcoll_id = unique_name(
              pipeline_components.pcollections, 'pcollection')
          pipeline_components.pcollections[precombined_pcoll_id].CopyFrom(
              beam_runner_api_pb2.PCollection(
                  unique_name=transform.unique_name + '/Precombine.out',
                  coder_id=windowed_coder_id(key_accumulator_coder_id),
                  windowing_strategy_id=input_pcoll.windowing_strategy_id,
                  is_bounded=input_pcoll.is_bounded))

          grouped_pcoll_id = unique_name(
              pipeline_components.pcollections, 'pcollection')
          pipeline_components.pcollections[grouped_pcoll_id].CopyFrom(
              beam_runner_api_pb2.PCollection(
                  unique_name=transform.unique_name + '/Group.out',
                  coder_id=windowed_coder_id(key_accumulator_iter_coder_id),
                  windowing_strategy_id=output_pcoll.windowing_strategy_id,
                  is_bounded=output_pcoll.is_bounded))

          merged_pcoll_id = unique_name(
              pipeline_components.pcollections, 'pcollection')
          pipeline_components.pcollections[merged_pcoll_id].CopyFrom(
              beam_runner_api_pb2.PCollection(
                  unique_name=transform.unique_name + '/Merge.out',
                  coder_id=windowed_coder_id(key_accumulator_coder_id),
                  windowing_strategy_id=output_pcoll.windowing_strategy_id,
                  is_bounded=output_pcoll.is_bounded))

          def make_stage(base_stage, transform):
            return Stage(
                transform.unique_name,
                [transform],
                downstream_side_inputs=base_stage.downstream_side_inputs,
                must_follow=base_stage.must_follow)

          yield make_stage(
              stage,
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Precombine',
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.COMBINE_PGBKCV_TRANSFORM,
                      payload=transform.spec.payload),
                  inputs=transform.inputs,
                  outputs={'out': precombined_pcoll_id}))

          yield make_stage(
              stage,
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Group',
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.GROUP_BY_KEY_TRANSFORM),
                  inputs={'in': precombined_pcoll_id},
                  outputs={'out': grouped_pcoll_id}))

          yield make_stage(
              stage,
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Merge',
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.COMBINE_MERGE_ACCUMULATORS_TRANSFORM,
                      payload=transform.spec.payload),
                  inputs={'in': grouped_pcoll_id},
                  outputs={'out': merged_pcoll_id}))

          yield make_stage(
              stage,
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/ExtractOutputs',
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.COMBINE_EXTRACT_OUTPUTS_TRANSFORM,
                      payload=transform.spec.payload),
                  inputs={'in': merged_pcoll_id},
                  outputs=transform.outputs))

        else:
          yield stage

    def expand_gbk(stages):
      """Transforms each GBK into a write followed by a read.
      """
      good_coder_urns = set(
          value for key, value in common_urns.__dict__.items()
          if re.match('[A-Z][A-Z_]*$', key))
      coders = pipeline_components.coders

      for coder_id, coder_proto in coders.items():
        if coder_proto.spec.spec.urn == common_urns.BYTES_CODER:
          bytes_coder_id = coder_id
          break
      else:
        bytes_coder_id = unique_name(coders, 'bytes_coder')
        pipeline_components.coders[bytes_coder_id].CopyFrom(
            beam.coders.BytesCoder().to_runner_api(None))

      coder_substitutions = {}

      def wrap_unknown_coders(coder_id, with_bytes):
        if (coder_id, with_bytes) not in coder_substitutions:
          wrapped_coder_id = None
          coder_proto = coders[coder_id]
          if coder_proto.spec.spec.urn == common_urns.LENGTH_PREFIX_CODER:
            coder_substitutions[coder_id, with_bytes] = (
                bytes_coder_id if with_bytes else coder_id)
          elif coder_proto.spec.spec.urn in good_coder_urns:
            wrapped_components = [wrap_unknown_coders(c, with_bytes)
                                  for c in coder_proto.component_coder_ids]
            if wrapped_components == list(coder_proto.component_coder_ids):
              # Use as is.
              coder_substitutions[coder_id, with_bytes] = coder_id
            else:
              wrapped_coder_id = unique_name(
                  coders,
                  coder_id + ("_bytes" if with_bytes else "_len_prefix"))
              coders[wrapped_coder_id].CopyFrom(coder_proto)
              coders[wrapped_coder_id].component_coder_ids[:] = [
                  wrap_unknown_coders(c, with_bytes)
                  for c in coder_proto.component_coder_ids]
              coder_substitutions[coder_id, with_bytes] = wrapped_coder_id
          else:
            # Not a known coder.
            if with_bytes:
              coder_substitutions[coder_id, with_bytes] = bytes_coder_id
            else:
              wrapped_coder_id = unique_name(coders, coder_id +  "_len_prefix")
              len_prefix_coder_proto = beam_runner_api_pb2.Coder(
                  spec=beam_runner_api_pb2.SdkFunctionSpec(
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=common_urns.LENGTH_PREFIX_CODER)),
                  component_coder_ids=[coder_id])
              coders[wrapped_coder_id].CopyFrom(len_prefix_coder_proto)
              coder_substitutions[coder_id, with_bytes] = wrapped_coder_id
          # This operation is idempotent.
          if wrapped_coder_id:
            coder_substitutions[wrapped_coder_id, with_bytes] = wrapped_coder_id
        return coder_substitutions[coder_id, with_bytes]

      def fix_pcoll_coder(pcoll):
        new_coder_id = wrap_unknown_coders(pcoll.coder_id, False)
        safe_coders[new_coder_id] = wrap_unknown_coders(pcoll.coder_id, True)
        pcoll.coder_id = new_coder_id

      for stage in stages:
        assert len(stage.transforms) == 1
        transform = stage.transforms[0]
        if transform.spec.urn == common_urns.GROUP_BY_KEY_TRANSFORM:
          for pcoll_id in transform.inputs.values():
            fix_pcoll_coder(pipeline_components.pcollections[pcoll_id])
          for pcoll_id in transform.outputs.values():
            fix_pcoll_coder(pipeline_components.pcollections[pcoll_id])

          # This is used later to correlate the read and write.
          param = str("group:%s" % stage.name)
          if stage.name not in pipeline_components.transforms:
            pipeline_components.transforms[stage.name].CopyFrom(transform)
          gbk_write = Stage(
              transform.unique_name + '/Write',
              [beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Write',
                  inputs=transform.inputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_OUTPUT_URN,
                      payload=param))],
              downstream_side_inputs=frozenset(),
              must_follow=stage.must_follow)
          yield gbk_write

          yield Stage(
              transform.unique_name + '/Read',
              [beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Read',
                  outputs=transform.outputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN,
                      payload=param))],
              downstream_side_inputs=stage.downstream_side_inputs,
              must_follow=union(frozenset([gbk_write]), stage.must_follow))
        else:
          yield stage

    def sink_flattens(stages):
      """Sink flattens and remove them from the graph.

      A flatten that cannot be sunk/fused away becomes multiple writes (to the
      same logical sink) followed by a read.
      """
      # TODO(robertwb): Actually attempt to sink rather than always materialize.
      # TODO(robertwb): Possibly fuse this into one of the stages.
      pcollections = pipeline_components.pcollections
      for stage in stages:
        assert len(stage.transforms) == 1
        transform = stage.transforms[0]
        if transform.spec.urn == common_urns.FLATTEN_TRANSFORM:
          # This is used later to correlate the read and writes.
          param = str("materialize:%s" % transform.unique_name)
          output_pcoll_id, = transform.outputs.values()
          output_coder_id = pcollections[output_pcoll_id].coder_id
          flatten_writes = []
          for local_in, pcoll_in in transform.inputs.items():

            if pcollections[pcoll_in].coder_id != output_coder_id:
              # Flatten inputs must all be written with the same coder as is
              # used to read them.
              pcollections[pcoll_in].coder_id = output_coder_id
              transcoded_pcollection = (
                  transform.unique_name + '/Transcode/' + local_in + '/out')
              yield Stage(
                  transform.unique_name + '/Transcode/' + local_in,
                  [beam_runner_api_pb2.PTransform(
                      unique_name=
                      transform.unique_name + '/Transcode/' + local_in,
                      inputs={local_in: pcoll_in},
                      outputs={'out': transcoded_pcollection},
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=bundle_processor.IDENTITY_DOFN_URN))],
                  downstream_side_inputs=frozenset(),
                  must_follow=stage.must_follow)
              pcollections[transcoded_pcollection].CopyFrom(
                  pcollections[pcoll_in])
              pcollections[transcoded_pcollection].coder_id = output_coder_id
            else:
              transcoded_pcollection = pcoll_in

            flatten_write = Stage(
                transform.unique_name + '/Write/' + local_in,
                [beam_runner_api_pb2.PTransform(
                    unique_name=transform.unique_name + '/Write/' + local_in,
                    inputs={local_in: transcoded_pcollection},
                    spec=beam_runner_api_pb2.FunctionSpec(
                        urn=bundle_processor.DATA_OUTPUT_URN,
                        payload=param))],
                downstream_side_inputs=frozenset(),
                must_follow=stage.must_follow)
            flatten_writes.append(flatten_write)
            yield flatten_write

          yield Stage(
              transform.unique_name + '/Read',
              [beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Read',
                  outputs=transform.outputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN,
                      payload=param))],
              downstream_side_inputs=stage.downstream_side_inputs,
              must_follow=union(frozenset(flatten_writes), stage.must_follow))

        else:
          yield stage

    def annotate_downstream_side_inputs(stages):
      """Annotate each stage with fusion-prohibiting information.

      Each stage is annotated with the (transitive) set of pcollections that
      depend on this stage that are also used later in the pipeline as a
      side input.

      While theoretically this could result in O(n^2) annotations, the size of
      each set is bounded by the number of side inputs (typically much smaller
      than the number of total nodes) and the number of *distinct* side-input
      sets is also generally small (and shared due to the use of union
      defined above).

      This representation is also amenable to simple recomputation on fusion.
      """
      consumers = collections.defaultdict(list)
      all_side_inputs = set()
      for stage in stages:
        for transform in stage.transforms:
          for input in transform.inputs.values():
            consumers[input].append(stage)
        for si in stage.side_inputs():
          all_side_inputs.add(si)
      all_side_inputs = frozenset(all_side_inputs)

      downstream_side_inputs_by_stage = {}

      def compute_downstream_side_inputs(stage):
        if stage not in downstream_side_inputs_by_stage:
          downstream_side_inputs = frozenset()
          for transform in stage.transforms:
            for output in transform.outputs.values():
              if output in all_side_inputs:
                downstream_side_inputs = union(
                    downstream_side_inputs, frozenset([output]))
              for consumer in consumers[output]:
                downstream_side_inputs = union(
                    downstream_side_inputs,
                    compute_downstream_side_inputs(consumer))
          downstream_side_inputs_by_stage[stage] = downstream_side_inputs
        return downstream_side_inputs_by_stage[stage]

      for stage in stages:
        stage.downstream_side_inputs = compute_downstream_side_inputs(stage)
      return stages

    def greedily_fuse(stages):
      """Places transforms sharing an edge in the same stage, whenever possible.
      """
      producers_by_pcoll = {}
      consumers_by_pcoll = collections.defaultdict(list)

      # Used to always reference the correct stage as the producer and
      # consumer maps are not updated when stages are fused away.
      replacements = {}

      def replacement(s):
        old_ss = []
        while s in replacements:
          old_ss.append(s)
          s = replacements[s]
        for old_s in old_ss[:-1]:
          replacements[old_s] = s
        return s

      def fuse(producer, consumer):
        fused = producer.fuse(consumer)
        replacements[producer] = fused
        replacements[consumer] = fused

      # First record the producers and consumers of each PCollection.
      for stage in stages:
        for transform in stage.transforms:
          for input in transform.inputs.values():
            consumers_by_pcoll[input].append(stage)
          for output in transform.outputs.values():
            producers_by_pcoll[output] = stage

      logging.debug('consumers\n%s', consumers_by_pcoll)
      logging.debug('producers\n%s', producers_by_pcoll)

      # Now try to fuse away all pcollections.
      for pcoll, producer in producers_by_pcoll.items():
        pcoll_as_param = str("materialize:%s" % pcoll)
        write_pcoll = None
        for consumer in consumers_by_pcoll[pcoll]:
          producer = replacement(producer)
          consumer = replacement(consumer)
          # Update consumer.must_follow set, as it's used in can_fuse.
          consumer.must_follow = frozenset(
              replacement(s) for s in consumer.must_follow)
          if producer.can_fuse(consumer):
            fuse(producer, consumer)
          else:
            # If we can't fuse, do a read + write.
            if write_pcoll is None:
              write_pcoll = Stage(
                  pcoll + '/Write',
                  [beam_runner_api_pb2.PTransform(
                      unique_name=pcoll + '/Write',
                      inputs={'in': pcoll},
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=bundle_processor.DATA_OUTPUT_URN,
                          payload=pcoll_as_param))])
              fuse(producer, write_pcoll)
            if consumer.has_as_main_input(pcoll):
              read_pcoll = Stage(
                  pcoll + '/Read',
                  [beam_runner_api_pb2.PTransform(
                      unique_name=pcoll + '/Read',
                      outputs={'out': pcoll},
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=bundle_processor.DATA_INPUT_URN,
                          payload=pcoll_as_param))],
                  must_follow=frozenset([write_pcoll]))
              fuse(read_pcoll, consumer)
            else:
              consumer.must_follow = union(
                  consumer.must_follow, frozenset([write_pcoll]))

      # Everything that was originally a stage or a replacement, but wasn't
      # replaced, should be in the final graph.
      final_stages = frozenset(stages).union(replacements.values()).difference(
          replacements.keys())

      for stage in final_stages:
        # Update all references to their final values before throwing
        # the replacement data away.
        stage.must_follow = frozenset(replacement(s) for s in stage.must_follow)
        # Two reads of the same stage may have been fused.  This is unneeded.
        stage.deduplicate_read()
      return final_stages

    def sort_stages(stages):
      """Order stages suitable for sequential execution.
      """
      seen = set()
      ordered = []

      def process(stage):
        if stage not in seen:
          seen.add(stage)
          for prev in stage.must_follow:
            process(prev)
          ordered.append(stage)
      for stage in stages:
        process(stage)
      return ordered

    # Now actually apply the operations.

    pipeline_components = copy.deepcopy(pipeline_proto.components)

    # Reify coders.
    # TODO(BEAM-2717): Remove once Coders are already in proto.
    coders = pipeline_context.PipelineContext(pipeline_components).coders
    for pcoll in pipeline_components.pcollections.values():
      if pcoll.coder_id not in coders:
        window_coder = coders[
            pipeline_components.windowing_strategies[
                pcoll.windowing_strategy_id].window_coder_id]
        coder = WindowedValueCoder(
            registry.get_coder(pickler.loads(pcoll.coder_id)),
            window_coder=window_coder)
        pcoll.coder_id = coders.get_id(coder)
    coders.populate_map(pipeline_components.coders)

    known_composites = set(
        [common_urns.GROUP_BY_KEY_TRANSFORM,
         common_urns.COMBINE_PER_KEY_TRANSFORM])

    def leaf_transforms(root_ids):
      for root_id in root_ids:
        root = pipeline_proto.components.transforms[root_id]
        if root.spec.urn in known_composites:
          yield root_id
        elif not root.subtransforms:
          # Make sure its outputs are not a subset of its inputs.
          if set(root.outputs.values()) - set(root.inputs.values()):
            yield root_id
        else:
          for leaf in leaf_transforms(root.subtransforms):
            yield leaf

    # Initial set of stages are singleton leaf transforms.
    stages = [
        Stage(name, [pipeline_proto.components.transforms[name]])
        for name in leaf_transforms(pipeline_proto.root_transform_ids)]

    # Apply each phase in order.
    for phase in [
        annotate_downstream_side_inputs, lift_combiners, expand_gbk,
        sink_flattens, greedily_fuse, sort_stages]:
      logging.info('%s %s %s', '=' * 20, phase, '=' * 20)
      stages = list(phase(stages))
      logging.debug('Stages: %s', [str(s) for s in stages])

    # Return the (possibly mutated) context and ordered set of stages.
    return pipeline_components, stages, safe_coders

  def run_stages(self, pipeline_components, stages, safe_coders):

    if self._use_grpc:
      controller = FnApiRunner.GrpcController(self._sdk_harness_factory)
    else:
      controller = FnApiRunner.DirectController()
    metrics_by_stage = {}

    try:
      pcoll_buffers = collections.defaultdict(list)
      for stage in stages:
        metrics_by_stage[stage.name] = self.run_stage(
            controller, pipeline_components, stage,
            pcoll_buffers, safe_coders).process_bundle.metrics
    finally:
      controller.close()

    return RunnerResult(runner.PipelineState.DONE, metrics_by_stage)

  def run_stage(
      self, controller, pipeline_components, stage, pcoll_buffers, safe_coders):

    context = pipeline_context.PipelineContext(pipeline_components)
    data_operation_spec = controller.data_operation_spec()

    def extract_endpoints(stage):
      # Returns maps of transform names to PCollection identifiers.
      # Also mutates IO stages to point to the data data_operation_spec.
      data_input = {}
      data_side_input = {}
      data_output = {}
      for transform in stage.transforms:
        if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                  bundle_processor.DATA_OUTPUT_URN):
          pcoll_id = transform.spec.payload
          if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
            target = transform.unique_name, only_element(transform.outputs)
            data_input[target] = pcoll_buffers[pcoll_id]
          elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
            target = transform.unique_name, only_element(transform.inputs)
            data_output[target] = pcoll_id
          else:
            raise NotImplementedError
          if data_operation_spec:
            transform.spec.payload = data_operation_spec.SerializeToString()
          else:
            transform.spec.payload = ""
        elif transform.spec.urn == common_urns.PARDO_TRANSFORM:
          payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
          for tag, si in payload.side_inputs.items():
            data_side_input[transform.unique_name, tag] = (
                'materialize:' + transform.inputs[tag],
                beam.pvalue.SideInputData.from_runner_api(si, None))
      return data_input, data_side_input, data_output

    logging.info('Running %s', stage.name)
    logging.debug('       %s', stage)
    data_input, data_side_input, data_output = extract_endpoints(stage)

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms={transform.unique_name: transform
                    for transform in stage.transforms},
        pcollections=dict(pipeline_components.pcollections.items()),
        coders=dict(pipeline_components.coders.items()),
        windowing_strategies=dict(
            pipeline_components.windowing_strategies.items()),
        environments=dict(pipeline_components.environments.items()))

    # Store the required side inputs into state.
    for (transform_id, tag), (pcoll_id, si) in data_side_input.items():
      elements_by_window = _WindowGroupingBuffer(si)
      for element_data in pcoll_buffers[pcoll_id]:
        elements_by_window.append(element_data)
      for window, elements_data in elements_by_window.items():
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                ptransform_id=transform_id,
                side_input_id=tag,
                window=window))
        controller.state_handler.blocking_append(state_key, elements_data, None)

    def get_buffer(pcoll_id):
      if pcoll_id.startswith('materialize:'):
        if pcoll_id not in pcoll_buffers:
          # Just store the data chunks for replay.
          pcoll_buffers[pcoll_id] = list()
      elif pcoll_id.startswith('group:'):
        # This is a grouping write, create a grouping buffer if needed.
        if pcoll_id not in pcoll_buffers:
          original_gbk_transform = pcoll_id.split(':', 1)[1]
          transform_proto = pipeline_components.transforms[
              original_gbk_transform]
          input_pcoll = only_element(transform_proto.inputs.values())
          output_pcoll = only_element(transform_proto.outputs.values())
          pre_gbk_coder = context.coders[safe_coders[
              pipeline_components.pcollections[input_pcoll].coder_id]]
          post_gbk_coder = context.coders[safe_coders[
              pipeline_components.pcollections[output_pcoll].coder_id]]
          windowing_strategy = context.windowing_strategies[
              pipeline_components
              .pcollections[output_pcoll].windowing_strategy_id]
          pcoll_buffers[pcoll_id] = _GroupingBuffer(
              pre_gbk_coder, post_gbk_coder, windowing_strategy)
      else:
        # These should be the only two identifiers we produce for now,
        # but special side input writes may go here.
        raise NotImplementedError(pcoll_id)
      return pcoll_buffers[pcoll_id]

    return BundleManager(
        controller, get_buffer, process_bundle_descriptor,
        self._progress_frequency).process_bundle(data_input, data_output)

  # These classes are used to interact with the worker.

  class StateServicer(beam_fn_api_pb2_grpc.BeamFnStateServicer):

    def __init__(self):
      self._lock = threading.Lock()
      self._state = collections.defaultdict(list)

    def blocking_get(self, state_key, instruction_reference=None):
      with self._lock:
        return ''.join(self._state[self._to_key(state_key)])

    def blocking_append(self, state_key, data, instruction_reference=None):
      with self._lock:
        self._state[self._to_key(state_key)].append(data)

    def blocking_clear(self, state_key, instruction_reference=None):
      with self._lock:
        del self._state[self._to_key(state_key)]

    @staticmethod
    def _to_key(state_key):
      return state_key.SerializeToString()

  class GrpcStateServicer(
      StateServicer, beam_fn_api_pb2_grpc.BeamFnStateServicer):
    def State(self, request_stream, context=None):
      # Note that this eagerly mutates state, assuming any failures are fatal.
      # Thus it is safe to ignore instruction_reference.
      for request in request_stream:
        if request.get:
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              get=beam_fn_api_pb2.StateGetResponse(
                  data=self.blocking_get(request.state_key)))
        elif request.append:
          self.blocking_append(request.state_key, request.append.data)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              append=beam_fn_api_pb2.AppendResponse())
        elif request.clear:
          self.blocking_clear(request.state_key)
          yield beam_fn_api_pb2.StateResponse(
              id=request.id,
              clear=beam_fn_api_pb2.ClearResponse())

  class DirectController(object):
    """An in-memory controller for fn API control, state and data planes."""

    def __init__(self):
      self.state_handler = FnApiRunner.StateServicer()
      self.control_handler = self
      self.data_plane_handler = data_plane.InMemoryDataChannel()
      self.worker = sdk_worker.SdkWorker(
          self.state_handler, data_plane.InMemoryDataChannelFactory(
              self.data_plane_handler.inverse()), {})
      self._uid_counter = 0

    def push(self, request):
      if not request.instruction_id:
        self._uid_counter += 1
        request.instruction_id = 'control_%s' % self._uid_counter
      logging.debug('CONTROL REQUEST %s', request)
      response = self.worker.do_instruction(request)
      logging.debug('CONTROL RESPONSE %s', response)
      return ControlFuture(request.instruction_id, response)

    def done(self):
      pass

    def close(self):
      pass

    def data_operation_spec(self):
      return None

  class GrpcController(object):
    """An grpc based controller for fn API control, state and data planes."""

    def __init__(self, sdk_harness_factory=None):
      self.sdk_harness_factory = sdk_harness_factory
      self.control_server = grpc.server(
          futures.ThreadPoolExecutor(max_workers=10))
      self.control_port = self.control_server.add_insecure_port('[::]:0')

      self.data_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
      self.data_port = self.data_server.add_insecure_port('[::]:0')

      self.control_handler = BeamFnControlServicer()
      beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
          self.control_handler, self.control_server)

      self.data_plane_handler = data_plane.GrpcServerDataChannel()
      beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
          self.data_plane_handler, self.data_server)

      # TODO(robertwb): Is sharing the control channel fine?  Alternatively,
      # how should this be plumbed?
      self.state_handler = FnApiRunner.GrpcStateServicer()
      beam_fn_api_pb2_grpc.add_BeamFnStateServicer_to_server(
          self.state_handler, self.control_server)

      logging.info('starting control server on port %s', self.control_port)
      logging.info('starting data server on port %s', self.data_port)
      self.data_server.start()
      self.control_server.start()

      self.worker = self.sdk_harness_factory(
          'localhost:%s' % self.control_port
      ) if self.sdk_harness_factory else sdk_worker.SdkHarness(
          'localhost:%s' % self.control_port, worker_count=1)

      self.worker_thread = threading.Thread(
          name='run_worker', target=self.worker.run)
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


class BundleManager(object):

  _uid_counter = 0

  def __init__(
      self, controller, get_buffer, bundle_descriptor, progress_frequency=None):
    self._controller = controller
    self._get_buffer = get_buffer
    self._bundle_descriptor = bundle_descriptor
    self._registered = False
    self._progress_frequency = progress_frequency

  def process_bundle(self, inputs, expected_outputs):
    # Unique id for the instruction processing this bundle.
    BundleManager._uid_counter += 1
    process_bundle_id = 'bundle_%s' % BundleManager._uid_counter

    # Register the bundle descriptor, if needed.
    if not self._registered:
      process_bundle_registration = beam_fn_api_pb2.InstructionRequest(
          register=beam_fn_api_pb2.RegisterRequest(
              process_bundle_descriptor=[self._bundle_descriptor]))
      self._controller.control_handler.push(process_bundle_registration)
      self._registered = True

    # Write all the input data to the channel.
    for (transform_id, name), elements in inputs.items():
      data_out = self._controller.data_plane_handler.output_stream(
          process_bundle_id, beam_fn_api_pb2.Target(
              primitive_transform_reference=transform_id, name=name))
      for element_data in elements:
        data_out.write(element_data)
      data_out.close()

    # Actually start the bundle.
    process_bundle = beam_fn_api_pb2.InstructionRequest(
        instruction_id=process_bundle_id,
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_reference=self._bundle_descriptor.id))
    result_future = self._controller.control_handler.push(process_bundle)

    with ProgressRequester(
        self._controller, process_bundle_id, self._progress_frequency):
      # Gather all output data.
      expected_targets = [
          beam_fn_api_pb2.Target(primitive_transform_reference=transform_id,
                                 name=output_name)
          for (transform_id, output_name), _ in expected_outputs.items()]
      logging.debug('Gather all output data from %s.', expected_targets)
      for output in self._controller.data_plane_handler.input_elements(
          process_bundle_id, expected_targets):
        target_tuple = (
            output.target.primitive_transform_reference, output.target.name)
        if target_tuple in expected_outputs:
          self._get_buffer(expected_outputs[target_tuple]).append(output.data)

      logging.debug('Wait for the bundle to finish.')
      result = result_future.get()

    if result.error:
      raise RuntimeError(result.error)
    return result


class ProgressRequester(threading.Thread):
  def __init__(self, controller, instruction_id, frequency, callback=None):
    super(ProgressRequester, self).__init__()
    self._controller = controller
    self._instruction_id = instruction_id
    self._frequency = frequency
    self._done = False
    self._latest_progress = None
    self._callback = callback
    self.daemon = True

  def __enter__(self):
    if self._frequency:
      self.start()

  def __exit__(self, *unused_exc_info):
    if self._frequency:
      self.stop()

  def run(self):
    while not self._done:
      try:
        progress_result = self._controller.control_handler.push(
            beam_fn_api_pb2.InstructionRequest(
                process_bundle_progress=
                beam_fn_api_pb2.ProcessBundleProgressRequest(
                    instruction_reference=self._instruction_id))).get()
        self._latest_progress = progress_result.process_bundle_progress
        if self._callback:
          self._callback(self._latest_progress)
      except Exception, exn:
        logging.error("Bad progress: %s", exn)
      time.sleep(self._frequency)

  def stop(self):
    self._done = True


class ControlFuture(object):
  def __init__(self, instruction_id, response=None):
    self.instruction_id = instruction_id
    if response:
      self._response = response
    else:
      self._response = None
      self._condition = threading.Condition()

  def set(self, response):
    with self._condition:
      self._response = response
      self._condition.notify_all()

  def get(self, timeout=None):
    if not self._response:
      with self._condition:
        if not self._response:
          self._condition.wait(timeout)
    return self._response


class FnApiMetrics(metrics.metric.MetricResults):
  def __init__(self, step_metrics):
    self._counters = {}
    self._distributions = {}
    self._gauges = {}
    for step_metric in step_metrics.values():
      for ptransform_id, ptransform in step_metric.ptransforms.items():
        for proto in ptransform.user:
          key = metrics.execution.MetricKey(
              ptransform_id,
              metrics.metricbase.MetricName.from_runner_api(proto.metric_name))
          if proto.HasField('counter_data'):
            self._counters[key] = proto.counter_data.value
          elif proto.HasField('distribution_data'):
            self._distributions[
                key] = metrics.cells.DistributionResult(
                    metrics.cells.DistributionData.from_runner_api(
                        proto.distribution_data))
          elif proto.HasField('gauge_data'):
            self._gauges[
                key] = metrics.cells.GaugeResult(
                    metrics.cells.GaugeData.from_runner_api(
                        proto.gauge_data))

  def query(self, filter=None):
    counters = [metrics.execution.MetricResult(k, v, v)
                for k, v in self._counters.items()
                if self.matches(filter, k)]
    distributions = [metrics.execution.MetricResult(k, v, v)
                     for k, v in self._distributions.items()
                     if self.matches(filter, k)]
    gauges = [metrics.execution.MetricResult(k, v, v)
              for k, v in self._gauges.items()
              if self.matches(filter, k)]

    return {'counters': counters,
            'distributions': distributions,
            'gauges': gauges}


class RunnerResult(runner.PipelineResult):
  def __init__(self, state, metrics_by_stage):
    super(RunnerResult, self).__init__(state)
    self._metrics_by_stage = metrics_by_stage
    self._user_metrics = None

  def wait_until_finish(self, duration=None):
    return self._state

  def metrics(self):
    if self._user_metrics is None:
      self._user_metrics = FnApiMetrics(self._metrics_by_stage)
    return self._user_metrics


def only_element(iterable):
  element, = iterable
  return element


def unique_name(existing, prefix):
  if prefix in existing:
    counter = 0
    while True:
      counter += 1
      prefix_counter = prefix + "_%s" % counter
      if prefix_counter not in existing:
        return prefix_counter
  else:
    return prefix
