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

"""Pipeline transformations for the FnApiRunner.
"""
from __future__ import absolute_import
from __future__ import print_function

import collections
import functools
import logging
from builtins import object

from apache_beam import coders
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils

# This module is experimental. No backwards-compatibility guarantees.


KNOWN_COMPOSITES = frozenset(
    [common_urns.primitives.GROUP_BY_KEY.urn,
     common_urns.composites.COMBINE_PER_KEY.urn])

IMPULSE_BUFFER = b'impulse'


class Stage(object):
  """A set of Transforms that can be sent to the worker for processing."""
  def __init__(self, name, transforms,
               downstream_side_inputs=None, must_follow=frozenset(),
               parent=None, environment=None):
    self.name = name
    self.transforms = transforms
    self.downstream_side_inputs = downstream_side_inputs
    self.must_follow = must_follow
    self.timer_pcollections = []
    self.parent = parent
    if environment is None:
      environment = functools.reduce(
          self._merge_environments,
          (self._extract_environment(t) for t in transforms))
    self.environment = environment

  def __repr__(self):
    must_follow = ', '.join(prev.name for prev in self.must_follow)
    if self.downstream_side_inputs is None:
      downstream_side_inputs = '<unknown>'
    else:
      downstream_side_inputs = ', '.join(
          str(si) for si in self.downstream_side_inputs)
    return "%s\n  %s\n  must follow: %s\n  downstream_side_inputs: %s" % (
        self.name,
        '\n'.join(["%s:%s" % (transform.unique_name, transform.spec.urn)
                   for transform in self.transforms]),
        must_follow,
        downstream_side_inputs)

  @staticmethod
  def _extract_environment(transform):
    if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
      pardo_payload = proto_utils.parse_Bytes(
          transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
      return pardo_payload.do_fn.environment_id
    elif transform.spec.urn in (
        common_urns.composites.COMBINE_PER_KEY.urn,
        common_urns.combine_components.COMBINE_PGBKCV.urn,
        common_urns.combine_components.COMBINE_MERGE_ACCUMULATORS.urn,
        common_urns.combine_components.COMBINE_EXTRACT_OUTPUTS.urn):
      combine_payload = proto_utils.parse_Bytes(
          transform.spec.payload, beam_runner_api_pb2.CombinePayload)
      return combine_payload.combine_fn.environment_id
    else:
      return None

  @staticmethod
  def _merge_environments(env1, env2):
    if env1 is None:
      return env2
    elif env2 is None:
      return env1
    else:
      if env1 != env2:
        raise ValueError("Incompatible environments: '%s' != '%s'" % (
            str(env1).replace('\n', ' '),
            str(env2).replace('\n', ' ')))
      return env1

  def can_fuse(self, consumer):
    try:
      self._merge_environments(self.environment, consumer.environment)
    except ValueError:
      return False

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
        union(self.must_follow, other.must_follow),
        environment=self._merge_environments(
            self.environment, other.environment))

  def is_flatten(self):
    return any(transform.spec.urn == common_urns.primitives.FLATTEN.urn
               for transform in self.transforms)

  def side_inputs(self):
    for transform in self.transforms:
      if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for side_input in payload.side_inputs:
          yield transform.inputs[side_input]

  def has_as_main_input(self, pcoll):
    for transform in self.transforms:
      if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
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
        pcoll = only_element(list(transform.outputs.items()))[1]
        if pcoll in seen_pcolls:
          continue
        seen_pcolls.add(pcoll)
      new_transforms.append(transform)
    self.transforms = new_transforms


def memoize_on_instance(f):
  missing = object()

  def wrapper(self, *args):
    try:
      cache = getattr(self, '_cache_%s' % f.__name__)
    except AttributeError:
      cache = {}
      setattr(self, '_cache_%s' % f.__name__, cache)
    result = cache.get(args, missing)
    if result is missing:
      result = cache[args] = f(self, *args)
    return result

  return wrapper


class TransformContext(object):

  _KNOWN_CODER_URNS = set(
      value.urn for value in common_urns.coders.__dict__.values())

  def __init__(self, components, use_state_iterables=False):
    self.components = components
    self.use_state_iterables = use_state_iterables
    self.safe_coders = {}
    self.bytes_coder_id = self.add_or_get_coder_id(
        coders.BytesCoder().to_runner_api(None), 'bytes_coder')

  def add_or_get_coder_id(self, coder_proto, coder_prefix='coder'):
    for coder_id, coder in self.components.coders.items():
      if coder == coder_proto:
        return coder_id
    new_coder_id = unique_name(self.components.coders, coder_prefix)
    self.components.coders[new_coder_id].CopyFrom(coder_proto)
    return new_coder_id

  @memoize_on_instance
  def with_state_iterables(self, coder_id):
    coder = self.components.coders[coder_id]
    if coder.spec.spec.urn == common_urns.coders.ITERABLE.urn:
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_state_backed')
      new_coder = self.components.coders[new_coder_id]
      new_coder.CopyFrom(coder)
      new_coder.spec.spec.urn = common_urns.coders.STATE_BACKED_ITERABLE.urn
      new_coder.spec.spec.payload = b'1'
      new_coder.component_coder_ids[0] = self.with_state_iterables(
          coder.component_coder_ids[0])
      return new_coder_id
    else:
      new_component_ids = [
          self.with_state_iterables(c) for c in coder.component_coder_ids]
      if new_component_ids == coder.component_coder_ids:
        return coder_id
      else:
        new_coder_id = unique_name(
            self.components.coders, coder_id + '_state_backed')
        self.components.coders[new_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec,
                component_coder_ids=new_component_ids))
        return new_coder_id

  @memoize_on_instance
  def length_prefixed_coder(self, coder_id):
    if coder_id in self.safe_coders:
      return coder_id
    length_prefixed_id, safe_id = self.length_prefixed_and_safe_coder(coder_id)
    self.safe_coders[length_prefixed_id] = safe_id
    return length_prefixed_id

  @memoize_on_instance
  def length_prefixed_and_safe_coder(self, coder_id):
    coder = self.components.coders[coder_id]
    if coder.spec.spec.urn == common_urns.coders.LENGTH_PREFIX.urn:
      return coder_id, self.bytes_coder_id
    elif coder.spec.spec.urn in self._KNOWN_CODER_URNS:
      new_component_ids = [
          self.length_prefixed_coder(c) for c in coder.component_coder_ids]
      if new_component_ids == coder.component_coder_ids:
        new_coder_id = coder_id
      else:
        new_coder_id = unique_name(
            self.components.coders, coder_id + '_length_prefixed')
        self.components.coders[new_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec,
                component_coder_ids=new_component_ids))
      safe_component_ids = [self.safe_coders[c] for c in new_component_ids]
      if safe_component_ids == coder.component_coder_ids:
        safe_coder_id = coder_id
      else:
        safe_coder_id = unique_name(
            self.components.coders, coder_id + '_safe')
        self.components.coders[safe_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec,
                component_coder_ids=safe_component_ids))
      return new_coder_id, safe_coder_id
    else:
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_length_prefixed')
      self.components.coders[new_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.SdkFunctionSpec(
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=common_urns.coders.LENGTH_PREFIX.urn)),
              component_coder_ids=[coder_id]))
      return new_coder_id, self.bytes_coder_id

  def length_prefix_pcoll_coders(self, pcoll_id):
    self.components.pcollections[pcoll_id].coder_id = (
        self.length_prefixed_coder(
            self.components.pcollections[pcoll_id].coder_id))


def leaf_transform_stages(
    root_ids, components, parent=None, known_composites=KNOWN_COMPOSITES):
  for root_id in root_ids:
    root = components.transforms[root_id]
    if root.spec.urn in known_composites:
      yield Stage(root_id, [root], parent=parent)
    elif not root.subtransforms:
      # Make sure its outputs are not a subset of its inputs.
      if set(root.outputs.values()) - set(root.inputs.values()):
        yield Stage(root_id, [root], parent=parent)
    else:
      for stage in leaf_transform_stages(
          root.subtransforms, components, root_id, known_composites):
        yield stage


def with_stages(pipeline_proto, stages):

  # In case it was a generator that mutates components as it
  # produces outputs (as is the case with most transformations).
  stages = list(stages)

  new_proto = beam_runner_api_pb2.Pipeline()
  new_proto.CopyFrom(pipeline_proto)
  components = new_proto.components
  components.transforms.clear()

  parents = {
      child: parent
      for parent, proto in pipeline_proto.components.transforms.items()
      for child in proto.subtransforms
  }

  def add_parent(child, parent):
    if parent not in components.transforms:
      components.transforms[parent].CopyFrom(
          pipeline_proto.components.transforms[parent])
      del components.transforms[parent].subtransforms[:]
      if parent in parents:
        add_parent(parent, parents[parent])
    components.transforms[parent].subtransforms.append(child)

  for stage in stages:
    for transform in stage.transforms:
      id = unique_name(components.transforms, stage.name)
      components.transforms[id].CopyFrom(transform)
      if stage.parent:
        add_parent(id, stage.parent)

  return new_proto


def annotate_downstream_side_inputs(stages, pipeline_context):
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


def fix_side_input_pcoll_coders(stages, pipeline_context):
  """Length prefix side input PCollection coders.
  """
  for stage in stages:
    for si in stage.side_inputs():
      pipeline_context.length_prefix_pcoll_coders(si)
  return stages


def lift_combiners(stages, context):
  """Expands CombinePerKey into pre- and post-grouping stages.

  ... -> CombinePerKey -> ...

  becomes

  ... -> PreCombine -> GBK -> MergeAccumulators -> ExtractOutput -> ...
  """
  for stage in stages:
    assert len(stage.transforms) == 1
    transform = stage.transforms[0]
    if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
      combine_payload = proto_utils.parse_Bytes(
          transform.spec.payload, beam_runner_api_pb2.CombinePayload)

      input_pcoll = context.components.pcollections[only_element(
          list(transform.inputs.values()))]
      output_pcoll = context.components.pcollections[only_element(
          list(transform.outputs.values()))]

      element_coder_id = input_pcoll.coder_id
      element_coder = context.components.coders[element_coder_id]
      key_coder_id, _ = element_coder.component_coder_ids
      accumulator_coder_id = combine_payload.accumulator_coder_id

      key_accumulator_coder = beam_runner_api_pb2.Coder(
          spec=beam_runner_api_pb2.SdkFunctionSpec(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.KV.urn)),
          component_coder_ids=[key_coder_id, accumulator_coder_id])
      key_accumulator_coder_id = context.add_or_get_coder_id(
          key_accumulator_coder)

      accumulator_iter_coder = beam_runner_api_pb2.Coder(
          spec=beam_runner_api_pb2.SdkFunctionSpec(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.ITERABLE.urn)),
          component_coder_ids=[accumulator_coder_id])
      accumulator_iter_coder_id = context.add_or_get_coder_id(
          accumulator_iter_coder)

      key_accumulator_iter_coder = beam_runner_api_pb2.Coder(
          spec=beam_runner_api_pb2.SdkFunctionSpec(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.KV.urn)),
          component_coder_ids=[key_coder_id, accumulator_iter_coder_id])
      key_accumulator_iter_coder_id = context.add_or_get_coder_id(
          key_accumulator_iter_coder)

      precombined_pcoll_id = unique_name(
          context.components.pcollections, 'pcollection')
      context.components.pcollections[precombined_pcoll_id].CopyFrom(
          beam_runner_api_pb2.PCollection(
              unique_name=transform.unique_name + '/Precombine.out',
              coder_id=key_accumulator_coder_id,
              windowing_strategy_id=input_pcoll.windowing_strategy_id,
              is_bounded=input_pcoll.is_bounded))

      grouped_pcoll_id = unique_name(
          context.components.pcollections, 'pcollection')
      context.components.pcollections[grouped_pcoll_id].CopyFrom(
          beam_runner_api_pb2.PCollection(
              unique_name=transform.unique_name + '/Group.out',
              coder_id=key_accumulator_iter_coder_id,
              windowing_strategy_id=output_pcoll.windowing_strategy_id,
              is_bounded=output_pcoll.is_bounded))

      merged_pcoll_id = unique_name(
          context.components.pcollections, 'pcollection')
      context.components.pcollections[merged_pcoll_id].CopyFrom(
          beam_runner_api_pb2.PCollection(
              unique_name=transform.unique_name + '/Merge.out',
              coder_id=key_accumulator_coder_id,
              windowing_strategy_id=output_pcoll.windowing_strategy_id,
              is_bounded=output_pcoll.is_bounded))

      def make_stage(base_stage, transform):
        return Stage(
            transform.unique_name,
            [transform],
            downstream_side_inputs=base_stage.downstream_side_inputs,
            must_follow=base_stage.must_follow,
            parent=base_stage.name)

      yield make_stage(
          stage,
          beam_runner_api_pb2.PTransform(
              unique_name=transform.unique_name + '/Precombine',
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.combine_components
                  .COMBINE_PER_KEY_PRECOMBINE.urn,
                  payload=transform.spec.payload),
              inputs=transform.inputs,
              outputs={'out': precombined_pcoll_id}))

      yield make_stage(
          stage,
          beam_runner_api_pb2.PTransform(
              unique_name=transform.unique_name + '/Group',
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.primitives.GROUP_BY_KEY.urn),
              inputs={'in': precombined_pcoll_id},
              outputs={'out': grouped_pcoll_id}))

      yield make_stage(
          stage,
          beam_runner_api_pb2.PTransform(
              unique_name=transform.unique_name + '/Merge',
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.combine_components
                  .COMBINE_PER_KEY_MERGE_ACCUMULATORS.urn,
                  payload=transform.spec.payload),
              inputs={'in': grouped_pcoll_id},
              outputs={'out': merged_pcoll_id}))

      yield make_stage(
          stage,
          beam_runner_api_pb2.PTransform(
              unique_name=transform.unique_name + '/ExtractOutputs',
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.combine_components
                  .COMBINE_PER_KEY_EXTRACT_OUTPUTS.urn,
                  payload=transform.spec.payload),
              inputs={'in': merged_pcoll_id},
              outputs=transform.outputs))

    else:
      yield stage


def expand_gbk(stages, pipeline_context):
  """Transforms each GBK into a write followed by a read.
  """
  for stage in stages:
    assert len(stage.transforms) == 1
    transform = stage.transforms[0]
    if transform.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
      for pcoll_id in transform.inputs.values():
        pipeline_context.length_prefix_pcoll_coders(pcoll_id)
      for pcoll_id in transform.outputs.values():
        if pipeline_context.use_state_iterables:
          pipeline_context.components.pcollections[
              pcoll_id].coder_id = pipeline_context.with_state_iterables(
                  pipeline_context.components.pcollections[pcoll_id].coder_id)
        pipeline_context.length_prefix_pcoll_coders(pcoll_id)

      # This is used later to correlate the read and write.
      transform_id = stage.name
      if transform != pipeline_context.components.transforms.get(transform_id):
        transform_id = unique_name(
            pipeline_context.components.transforms, stage.name)
        pipeline_context.components.transforms[transform_id].CopyFrom(transform)
      grouping_buffer = create_buffer_id(transform_id, kind='group')
      gbk_write = Stage(
          transform.unique_name + '/Write',
          [beam_runner_api_pb2.PTransform(
              unique_name=transform.unique_name + '/Write',
              inputs=transform.inputs,
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=bundle_processor.DATA_OUTPUT_URN,
                  payload=grouping_buffer))],
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
                  payload=grouping_buffer))],
          downstream_side_inputs=stage.downstream_side_inputs,
          must_follow=union(frozenset([gbk_write]), stage.must_follow))
    else:
      yield stage


def sink_flattens(stages, pipeline_context):
  """Sink flattens and remove them from the graph.

  A flatten that cannot be sunk/fused away becomes multiple writes (to the
  same logical sink) followed by a read.
  """
  # TODO(robertwb): Actually attempt to sink rather than always materialize.
  # TODO(robertwb): Possibly fuse this into one of the stages.
  pcollections = pipeline_context.components.pcollections
  for stage in stages:
    assert len(stage.transforms) == 1
    transform = stage.transforms[0]
    if transform.spec.urn == common_urns.primitives.FLATTEN.urn:
      # This is used later to correlate the read and writes.
      buffer_id = create_buffer_id(transform.unique_name)
      output_pcoll_id, = list(transform.outputs.values())
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
                    payload=buffer_id))],
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
                  payload=buffer_id))],
          downstream_side_inputs=stage.downstream_side_inputs,
          must_follow=union(frozenset(flatten_writes), stage.must_follow))

    else:
      yield stage


def greedily_fuse(stages, pipeline_context):
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
        buffer_id = create_buffer_id(pcoll)
        if write_pcoll is None:
          write_pcoll = Stage(
              pcoll + '/Write',
              [beam_runner_api_pb2.PTransform(
                  unique_name=pcoll + '/Write',
                  inputs={'in': pcoll},
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_OUTPUT_URN,
                      payload=buffer_id))])
          fuse(producer, write_pcoll)
        if consumer.has_as_main_input(pcoll):
          read_pcoll = Stage(
              pcoll + '/Read',
              [beam_runner_api_pb2.PTransform(
                  unique_name=pcoll + '/Read',
                  outputs={'out': pcoll},
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN,
                      payload=buffer_id))],
              must_follow=frozenset([write_pcoll]))
          fuse(read_pcoll, consumer)
        else:
          consumer.must_follow = union(
              consumer.must_follow, frozenset([write_pcoll]))

  # Everything that was originally a stage or a replacement, but wasn't
  # replaced, should be in the final graph.
  final_stages = frozenset(stages).union(list(replacements.values()))\
      .difference(list(replacements))

  for stage in final_stages:
    # Update all references to their final values before throwing
    # the replacement data away.
    stage.must_follow = frozenset(replacement(s) for s in stage.must_follow)
    # Two reads of the same stage may have been fused.  This is unneeded.
    stage.deduplicate_read()
  return final_stages


def read_to_impulse(stages, pipeline_context):
  """Translates Read operations into Impulse operations."""
  for stage in stages:
    # First map Reads, if any, to Impulse + triggered read op.
    for transform in list(stage.transforms):
      if transform.spec.urn == common_urns.deprecated_primitives.READ.urn:
        read_pc = only_element(transform.outputs.values())
        read_pc_proto = pipeline_context.components.pcollections[read_pc]
        impulse_pc = unique_name(
            pipeline_context.components.pcollections, 'Impulse')
        pipeline_context.components.pcollections[impulse_pc].CopyFrom(
            beam_runner_api_pb2.PCollection(
                unique_name=impulse_pc,
                coder_id=pipeline_context.bytes_coder_id,
                windowing_strategy_id=read_pc_proto.windowing_strategy_id,
                is_bounded=read_pc_proto.is_bounded))
        stage.transforms.remove(transform)
        # TODO(robertwb): If this goes multi-process before fn-api
        # read is default, expand into split + reshuffle + read.
        stage.transforms.append(
            beam_runner_api_pb2.PTransform(
                unique_name=transform.unique_name + '/Impulse',
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=common_urns.primitives.IMPULSE.urn),
                outputs={'out': impulse_pc}))
        stage.transforms.append(
            beam_runner_api_pb2.PTransform(
                unique_name=transform.unique_name,
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=python_urns.IMPULSE_READ_TRANSFORM,
                    payload=transform.spec.payload),
                inputs={'in': impulse_pc},
                outputs={'out': read_pc}))

    yield stage


def impulse_to_input(stages, pipeline_context):
  """Translates Impulse operations into GRPC reads."""
  for stage in stages:
    for transform in list(stage.transforms):
      if transform.spec.urn == common_urns.primitives.IMPULSE.urn:
        stage.transforms.remove(transform)
        stage.transforms.append(
            beam_runner_api_pb2.PTransform(
                unique_name=transform.unique_name,
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=bundle_processor.DATA_INPUT_URN,
                    payload=IMPULSE_BUFFER),
                outputs=transform.outputs))
    yield stage


def inject_timer_pcollections(stages, pipeline_context):
  """Create PCollections for fired timers and to-be-set timers.

  At execution time, fired timers and timers-to-set are represented as
  PCollections that are managed by the runner.  This phase adds the
  necissary collections, with their read and writes, to any stages using
  timers.
  """
  for stage in stages:
    for transform in list(stage.transforms):
      if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for tag, spec in payload.timer_specs.items():
          if len(transform.inputs) > 1:
            raise NotImplementedError('Timers and side inputs.')
          input_pcoll = pipeline_context.components.pcollections[
              next(iter(transform.inputs.values()))]
          # Create the appropriate coder for the timer PCollection.
          key_coder_id = input_pcoll.coder_id
          if (pipeline_context.components.coders[key_coder_id].spec.spec.urn
              == common_urns.coders.KV.urn):
            key_coder_id = pipeline_context.components.coders[
                key_coder_id].component_coder_ids[0]
          key_timer_coder_id = pipeline_context.add_or_get_coder_id(
              beam_runner_api_pb2.Coder(
                  spec=beam_runner_api_pb2.SdkFunctionSpec(
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=common_urns.coders.KV.urn)),
                  component_coder_ids=[key_coder_id, spec.timer_coder_id]))
          # Inject the read and write pcollections.
          timer_read_pcoll = unique_name(
              pipeline_context.components.pcollections,
              '%s_timers_to_read_%s' % (transform.unique_name, tag))
          timer_write_pcoll = unique_name(
              pipeline_context.components.pcollections,
              '%s_timers_to_write_%s' % (transform.unique_name, tag))
          pipeline_context.components.pcollections[timer_read_pcoll].CopyFrom(
              beam_runner_api_pb2.PCollection(
                  unique_name=timer_read_pcoll,
                  coder_id=key_timer_coder_id,
                  windowing_strategy_id=input_pcoll.windowing_strategy_id,
                  is_bounded=input_pcoll.is_bounded))
          pipeline_context.components.pcollections[timer_write_pcoll].CopyFrom(
              beam_runner_api_pb2.PCollection(
                  unique_name=timer_write_pcoll,
                  coder_id=key_timer_coder_id,
                  windowing_strategy_id=input_pcoll.windowing_strategy_id,
                  is_bounded=input_pcoll.is_bounded))
          stage.transforms.append(
              beam_runner_api_pb2.PTransform(
                  unique_name=timer_read_pcoll + '/Read',
                  outputs={'out': timer_read_pcoll},
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN,
                      payload=create_buffer_id(
                          timer_read_pcoll, kind='timers'))))
          stage.transforms.append(
              beam_runner_api_pb2.PTransform(
                  unique_name=timer_write_pcoll + '/Write',
                  inputs={'in': timer_write_pcoll},
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_OUTPUT_URN,
                      payload=create_buffer_id(
                          timer_write_pcoll, kind='timers'))))
          assert tag not in transform.inputs
          transform.inputs[tag] = timer_read_pcoll
          assert tag not in transform.outputs
          transform.outputs[tag] = timer_write_pcoll
          stage.timer_pcollections.append(
              (timer_read_pcoll + '/Read', timer_write_pcoll))
    yield stage


def sort_stages(stages, pipeline_context):
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


def window_pcollection_coders(stages, pipeline_context):
  """Wrap all PCollection coders as windowed value coders.

  This is required as some SDK workers require windowed coders for their
  PCollections.
  TODO(BEAM-4150): Consistently use unwindowed coders everywhere.
  """
  def windowed_coder_id(coder_id, window_coder_id):
    proto = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.SdkFunctionSpec(
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=common_urns.coders.WINDOWED_VALUE.urn)),
        component_coder_ids=[coder_id, window_coder_id])
    return pipeline_context.add_or_get_coder_id(
        proto, coder_id + '_windowed')

  for pcoll in pipeline_context.components.pcollections.values():
    if (pipeline_context.components.coders[pcoll.coder_id].spec.spec.urn
        != common_urns.coders.WINDOWED_VALUE.urn):
      original_coder_id = pcoll.coder_id
      pcoll.coder_id = windowed_coder_id(
          pcoll.coder_id,
          pipeline_context.components.windowing_strategies[
              pcoll.windowing_strategy_id].window_coder_id)
      if (original_coder_id in pipeline_context.safe_coders
          and pcoll.coder_id not in pipeline_context.safe_coders):
        # TODO: This assumes the window coder is safe.
        pipeline_context.safe_coders[pcoll.coder_id] = windowed_coder_id(
            pipeline_context.safe_coders[original_coder_id],
            pipeline_context.components.windowing_strategies[
                pcoll.windowing_strategy_id].window_coder_id)

  return stages


def union(a, b):
  # Minimize the number of distinct sets.
  if not a or a == b:
    return b
  elif not b:
    return a
  else:
    return frozenset.union(a, b)


_global_counter = 0


def unique_name(existing, prefix):
  if existing is None:
    global _global_counter
    _global_counter += 1
    return '%s_%d' % (prefix, _global_counter)
  elif prefix in existing:
    counter = 0
    while True:
      counter += 1
      prefix_counter = prefix + "_%s" % counter
      if prefix_counter not in existing:
        return prefix_counter
  else:
    return prefix


def only_element(iterable):
  element, = iterable
  return element


def create_buffer_id(name, kind='materialize'):
  return ('%s:%s' % (kind, name)).encode('utf-8')


def split_buffer_id(buffer_id):
  return buffer_id.decode('utf-8').split(':', 1)
