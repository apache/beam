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
import copy
import logging
import Queue as queue
import threading
import time
from concurrent import futures

import grpc

import apache_beam as beam  # pylint: disable=ungrouped-imports
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders import registry
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import maptask_executor_runner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import sdk_worker
from apache_beam.transforms import trigger
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import proto_utils
from apache_beam.utils import urns

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
      self._started = False

    def run(self, iterator, context):
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
        self._pull_queue.put(data)

    def push(self, item):
      self._push_queue.put(item)

    def pull(self, timeout=None):
      return self._pull_queue.get(timeout=timeout)

    def empty(self):
      return self._pull_queue.empty()

    def done(self):
      self.push(self._DONE)
      # Can't join a thread before it's started.
      while not self._started:
        time.sleep(.01)
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
    while input_stream.size() > 0:
      windowed_key_value = self._pre_grouped_coder.get_impl(
          ).decode_from_stream(input_stream, True)
      key = windowed_key_value.value[0]
      windowed_value = windowed_key_value.with_value(
          windowed_key_value.value[1])
      self._table[self._key_coder.encode(key)].append(windowed_value)

  def __iter__(self):
    output_stream = create_OutputStream()
    trigger_driver = trigger.create_trigger_driver(self._windowing, True)
    for encoded_key, windowed_values in self._table.items():
      key = self._key_coder.decode(encoded_key)
      for wkvs in trigger_driver.process_entire_key(key, windowed_values):
        self._post_grouped_coder.get_impl().encode_to_stream(
            wkvs, output_stream, True)
    return iter([output_stream.get()])


class FnApiRunner(maptask_executor_runner.MapTaskExecutorRunner):

  def __init__(self, use_grpc=False, sdk_harness_factory=None):
    super(FnApiRunner, self).__init__()
    self._last_uid = -1
    self._use_grpc = use_grpc
    if sdk_harness_factory and not use_grpc:
      raise ValueError('GRPC must be used if a harness factory is provided.')
    self._sdk_harness_factory = sdk_harness_factory

  def has_metrics_support(self):
    return False

  def _next_uid(self):
    self._last_uid += 1
    return str(self._last_uid)

  def run(self, pipeline):
    MetricsEnvironment.set_metrics_supported(self.has_metrics_support())
    if pipeline._verify_runner_api_compatible():
      return self.run_via_runner_api(pipeline.to_runner_api())
    else:
      return super(FnApiRunner, self).run(pipeline)

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
        return "%s\n    %s\n    must follow: %s" % (
            self.name,
            '\n'.join(["%s:%s" % (transform.unique_name, transform.spec.urn)
                       for transform in self.transforms]),
            must_follow)

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
        return any(transform.spec.urn == urns.FLATTEN_TRANSFORM
                   for transform in self.transforms)

      def side_inputs(self):
        for transform in self.transforms:
          if transform.spec.urn == urns.PARDO_TRANSFORM:
            payload = proto_utils.parse_Bytes(
                transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
            for side_input in payload.side_inputs:
              yield transform.inputs[side_input]

      def has_as_main_input(self, pcoll):
        for transform in self.transforms:
          if transform.spec.urn == urns.PARDO_TRANSFORM:
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

    def expand_gbk(stages):
      """Transforms each GBK into a write followed by a read.
      """
      good_coder_urns = set(beam.coders.Coder._known_urns.keys()) - set([
          urns.PICKLED_CODER])
      coders = pipeline_components.coders

      for coder_id, coder_proto in coders.items():
        if coder_proto.spec.spec.urn == urns.BYTES_CODER:
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
          if coder_proto.spec.spec.urn == urns.LENGTH_PREFIX_CODER:
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
                          urn=urns.LENGTH_PREFIX_CODER)),
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
        if transform.spec.urn == urns.GROUP_BY_KEY_TRANSFORM:
          for pcoll_id in transform.inputs.values():
            fix_pcoll_coder(pipeline_components.pcollections[pcoll_id])
          for pcoll_id in transform.outputs.values():
            fix_pcoll_coder(pipeline_components.pcollections[pcoll_id])

          # This is used later to correlate the read and write.
          param = str("group:%s" % stage.name)
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
              downstream_side_inputs=frozenset(),
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
        if transform.spec.urn == urns.FLATTEN_TRANSFORM:
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
              downstream_side_inputs=frozenset(),
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
                downstream_side_inputs = union(downstream_side_inputs, output)
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
          consumer.must_follow = set(
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
                  must_follow={write_pcoll})
              fuse(read_pcoll, consumer)

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

    known_composites = set([urns.GROUP_BY_KEY_TRANSFORM])

    def leaf_transforms(root_ids):
      for root_id in root_ids:
        root = pipeline_proto.components.transforms[root_id]
        if root.spec.urn in known_composites or not root.subtransforms:
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
        annotate_downstream_side_inputs, expand_gbk, sink_flattens,
        greedily_fuse, sort_stages]:
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

    try:
      pcoll_buffers = collections.defaultdict(list)
      for stage in stages:
        self.run_stage(
            controller, pipeline_components, stage, pcoll_buffers, safe_coders)
    finally:
      controller.close()

    return maptask_executor_runner.WorkerRunnerResult(PipelineState.DONE)

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
        pcoll_id = transform.spec.payload
        if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                  bundle_processor.DATA_OUTPUT_URN):
          if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
            target = transform.unique_name, only_element(transform.outputs)
            data_input[target] = pcoll_id
          elif transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
            target = transform.unique_name, only_element(transform.inputs)
            data_output[target] = pcoll_id
          else:
            raise NotImplementedError
          if data_operation_spec:
            transform.spec.payload = data_operation_spec.SerializeToString()
          else:
            transform.spec.payload = ""
      return data_input, data_side_input, data_output

    logging.info('Running %s', stage.name)
    logging.debug('       %s', stage)
    data_input, data_side_input, data_output = extract_endpoints(stage)
    if data_side_input:
      raise NotImplementedError('Side inputs.')

    process_bundle_descriptor = beam_fn_api_pb2.ProcessBundleDescriptor(
        id=self._next_uid(),
        transforms={transform.unique_name: transform
                    for transform in stage.transforms},
        pcollections=dict(pipeline_components.pcollections.items()),
        coders=dict(pipeline_components.coders.items()),
        windowing_strategies=dict(
            pipeline_components.windowing_strategies.items()),
        environments=dict(pipeline_components.environments.items()))

    process_bundle_registration = beam_fn_api_pb2.InstructionRequest(
        instruction_id=self._next_uid(),
        register=beam_fn_api_pb2.RegisterRequest(
            process_bundle_descriptor=[process_bundle_descriptor]))

    process_bundle = beam_fn_api_pb2.InstructionRequest(
        instruction_id=self._next_uid(),
        process_bundle=beam_fn_api_pb2.ProcessBundleRequest(
            process_bundle_descriptor_reference=
            process_bundle_descriptor.id))

    # Write all the input data to the channel.
    for (transform_id, name), pcoll_id in data_input.items():
      data_out = controller.data_plane_handler.output_stream(
          process_bundle.instruction_id, beam_fn_api_pb2.Target(
              primitive_transform_reference=transform_id, name=name))
      for element_data in pcoll_buffers[pcoll_id]:
        data_out.write(element_data)
      data_out.close()

    # Register and start running the bundle.
    controller.control_handler.push(process_bundle_registration)
    controller.control_handler.push(process_bundle)

    # Wait for the bundle to finish.
    while True:
      result = controller.control_handler.pull()
      if result and result.instruction_id == process_bundle.instruction_id:
        if result.error:
          raise RuntimeError(result.error)
        break

    # Gather all output data.
    expected_targets = [
        beam_fn_api_pb2.Target(primitive_transform_reference=transform_id,
                               name=output_name)
        for (transform_id, output_name), _ in data_output.items()]
    for output in controller.data_plane_handler.input_elements(
        process_bundle.instruction_id, expected_targets):
      target_tuple = (
          output.target.primitive_transform_reference, output.target.name)
      if target_tuple in data_output:
        pcoll_id = data_output[target_tuple]
        if pcoll_id.startswith('materialize:'):
          # Just store the data chunks for replay.
          pcoll_buffers[pcoll_id].append(output.data)
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
          pcoll_buffers[pcoll_id].append(output.data)
        else:
          # These should be the only two identifiers we produce for now,
          # but special side input writes may go here.
          raise NotImplementedError(pcoll_id)

  # This is the "old" way of executing pipelines.
  # TODO(robertwb): Remove once runner API supports side inputs.

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
        target_name = only_element(get_inputs(operation).keys())
        runner_sinks[(transform_id, target_name)] = operation
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=bundle_processor.DATA_OUTPUT_URN,
            payload=data_operation_spec.SerializeToString() \
                if data_operation_spec is not None else None)

      elif isinstance(operation, operation_specs.WorkerRead):
        # A Read from an in-memory source is done over the data plane.
        if (isinstance(operation.source.source,
                       maptask_executor_runner.InMemorySource)
            and isinstance(operation.source.source.default_output_coder(),
                           WindowedValueCoder)):
          target_name = only_element(get_outputs(op_ix).keys())
          input_data[(transform_id, target_name)] = self._reencode_elements(
              operation.source.source.read(None),
              operation.source.source.default_output_coder())
          transform_spec = beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.DATA_INPUT_URN,
              payload=data_operation_spec.SerializeToString() \
                  if data_operation_spec is not None else None)

        else:
          # Otherwise serialize the source and execute it there.
          # TODO: Use SDFs with an initial impulse.
          # The Dataflow runner harness strips the base64 encoding. do the same
          # here until we get the same thing back that we sent in.
          source_bytes = base64.b64decode(
              pickler.dumps(operation.source.source))
          transform_spec = beam_runner_api_pb2.FunctionSpec(
              urn=bundle_processor.PYTHON_SOURCE_URN,
              payload=source_bytes)

      elif isinstance(operation, operation_specs.WorkerDoFn):
        # Record the contents of each side input for access via the state api.
        side_input_extras = []
        for si in operation.side_inputs:
          assert isinstance(si.source, iobase.BoundedSource)
          element_coder = si.source.default_output_coder()
          # TODO(robertwb): Actually flesh out the ViewFn API.
          side_input_extras.append((si.tag, element_coder))
          side_input_data[
              bundle_processor.side_input_tag(transform_id, si.tag)] = (
                  self._reencode_elements(
                      si.source.read(si.source.get_range_tracker(None, None)),
                      element_coder))
        augmented_serialized_fn = pickler.dumps(
            (operation.serialized_fn, side_input_extras))
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=bundle_processor.PYTHON_DOFN_URN,
            payload=augmented_serialized_fn)

      elif isinstance(operation, operation_specs.WorkerFlatten):
        # Flatten is nice and simple.
        transform_spec = beam_runner_api_pb2.FunctionSpec(
            urn=bundle_processor.IDENTITY_DOFN_URN)

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
        coders=dict(context_proto.coders.items()),
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

  def execute_map_tasks(self, ordered_map_tasks, direct=False):
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

  @staticmethod
  def _reencode_elements(elements, element_coder):
    output_stream = create_OutputStream()
    for element in elements:
      element_coder.get_impl().encode_to_stream(element, output_stream, True)
    return output_stream.get()

  # These classes are used to interact with the worker.

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

    def __init__(self, sdk_harness_factory=None):
      self.sdk_harness_factory = sdk_harness_factory
      self.state_handler = FnApiRunner.SimpleState()
      self.control_server = grpc.server(
          futures.ThreadPoolExecutor(max_workers=10))
      self.control_port = self.control_server.add_insecure_port('[::]:0')

      self.data_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
      self.data_port = self.data_server.add_insecure_port('[::]:0')

      self.control_handler = streaming_rpc_handler(
          beam_fn_api_pb2_grpc.BeamFnControlServicer, 'Control')
      beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
          self.control_handler, self.control_server)

      self.data_plane_handler = data_plane.GrpcServerDataChannel()
      beam_fn_api_pb2_grpc.add_BeamFnDataServicer_to_server(
          self.data_plane_handler, self.data_server)

      logging.info('starting control server on port %s', self.control_port)
      logging.info('starting data server on port %s', self.data_port)
      self.data_server.start()
      self.control_server.start()

      self.worker = (self.sdk_harness_factory or sdk_worker.SdkHarness)(
          'localhost:%s' % self.control_port)
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
