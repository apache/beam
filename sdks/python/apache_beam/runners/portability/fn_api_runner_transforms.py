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

from builtins import object

from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils

# This module is experimental. No backwards-compatibility guarantees.


KNOWN_COMPOSITES = frozenset(
    [common_urns.primitives.GROUP_BY_KEY.urn,
     common_urns.composites.COMBINE_PER_KEY.urn])


class Stage(object):
  """A set of Transforms that can be sent to the worker for processing."""
  def __init__(self, name, transforms,
               downstream_side_inputs=None, must_follow=frozenset(),
               parent=None):
    self.name = name
    self.transforms = transforms
    self.downstream_side_inputs = downstream_side_inputs
    self.must_follow = must_follow
    self.timer_pcollections = []
    self.parent = parent

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


class TransformContext(object):
  def __init__(self, components):
    self.components = components

  def add_or_get_coder_id(self, coder_proto):
    for coder_id, coder in self.components.coders.items():
      if coder == coder_proto:
        return coder_id
    new_coder_id = unique_name(self.components.coders, 'coder')
    self.components.coders[new_coder_id].CopyFrom(coder_proto)
    return new_coder_id


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


def union(a, b):
  # Minimize the number of distinct sets.
  if not a or a == b:
    return b
  elif not b:
    return a
  else:
    return frozenset.union(a, b)


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


def only_element(iterable):
  element, = iterable
  return element
