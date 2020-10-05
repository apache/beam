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
# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import collections
import functools
import itertools
import logging
from builtins import object
from typing import Container
from typing import DefaultDict
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar

from past.builtins import unicode

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.utils import proto_utils

T = TypeVar('T')

# This module is experimental. No backwards-compatibility guarantees.

_LOGGER = logging.getLogger(__name__)

KNOWN_COMPOSITES = frozenset([
    common_urns.primitives.GROUP_BY_KEY.urn,
    common_urns.composites.COMBINE_PER_KEY.urn,
    common_urns.primitives.PAR_DO.urn,  # After SDF expansion.
])

COMBINE_URNS = frozenset([
    common_urns.composites.COMBINE_PER_KEY.urn,
])

PAR_DO_URNS = frozenset([
    common_urns.primitives.PAR_DO.urn,
    common_urns.sdf_components.PAIR_WITH_RESTRICTION.urn,
    common_urns.sdf_components.SPLIT_AND_SIZE_RESTRICTIONS.urn,
    common_urns.sdf_components.PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS.urn,
    common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn,
])

IMPULSE_BUFFER = b'impulse'

# SideInputId is identified by a consumer ParDo + tag.
SideInputId = Tuple[str, str]
SideInputAccessPattern = beam_runner_api_pb2.FunctionSpec

DataOutput = Dict[str, bytes]

# DataSideInput maps SideInputIds to a tuple of the encoded bytes of the side
# input content, and a payload specification regarding the type of side input
# (MultiMap / Iterable).
DataSideInput = Dict[SideInputId, Tuple[bytes, SideInputAccessPattern]]


class Stage(object):
  """A set of Transforms that can be sent to the worker for processing."""
  def __init__(
      self,
      name,  # type: str
      transforms,  # type: List[beam_runner_api_pb2.PTransform]
      downstream_side_inputs=None,  # type: Optional[FrozenSet[str]]
      must_follow=frozenset(),  # type: FrozenSet[Stage]
      parent=None,  # type: Optional[Stage]
      environment=None,  # type: Optional[str]
      forced_root=False):
    self.name = name
    self.transforms = transforms
    self.downstream_side_inputs = downstream_side_inputs
    self.must_follow = must_follow
    self.timers = set()  # type: Set[Tuple[str, str]]
    self.parent = parent
    if environment is None:
      environment = functools.reduce(
          self._merge_environments,
          (self._extract_environment(t) for t in transforms))
    self.environment = environment
    self.forced_root = forced_root

  def __repr__(self):
    must_follow = ', '.join(prev.name for prev in self.must_follow)
    if self.downstream_side_inputs is None:
      downstream_side_inputs = '<unknown>'
    else:
      downstream_side_inputs = ', '.join(
          str(si) for si in self.downstream_side_inputs)
    return "%s\n  %s\n  must follow: %s\n  downstream_side_inputs: %s" % (
        self.name,
        '\n'.join([
            "%s:%s" % (transform.unique_name, transform.spec.urn)
            for transform in self.transforms
        ]),
        must_follow,
        downstream_side_inputs)

  @staticmethod
  def _extract_environment(transform):
    # type: (beam_runner_api_pb2.PTransform) -> Optional[str]
    environment = transform.environment_id
    return environment if environment else None

  @staticmethod
  def _merge_environments(env1, env2):
    # type: (Optional[str], Optional[str]) -> Optional[str]
    if env1 is None:
      return env2
    elif env2 is None:
      return env1
    else:
      if env1 != env2:
        raise ValueError(
            "Incompatible environments: '%s' != '%s'" %
            (str(env1).replace('\n', ' '), str(env2).replace('\n', ' ')))
      return env1

  def can_fuse(self, consumer, context):
    # type: (Stage, TransformContext) -> bool
    try:
      self._merge_environments(self.environment, consumer.environment)
    except ValueError:
      return False

    def no_overlap(a, b):
      return not a or not b or not a.intersection(b)

    return (
        not consumer.forced_root and not self in consumer.must_follow and
        self.is_all_sdk_urns(context) and consumer.is_all_sdk_urns(context) and
        no_overlap(self.downstream_side_inputs, consumer.side_inputs()))

  def fuse(self, other):
    # type: (Stage) -> Stage
    return Stage(
        "(%s)+(%s)" % (self.name, other.name),
        self.transforms + other.transforms,
        union(self.downstream_side_inputs, other.downstream_side_inputs),
        union(self.must_follow, other.must_follow),
        environment=self._merge_environments(
            self.environment, other.environment),
        parent=self.parent if self.parent == other.parent else None,
        forced_root=self.forced_root or other.forced_root)

  def is_runner_urn(self, context):
    # type: (TransformContext) -> bool
    return any(
        transform.spec.urn in context.known_runner_urns
        for transform in self.transforms)

  def is_all_sdk_urns(self, context):
    def is_sdk_transform(transform):
      # Execute multi-input flattens in the runner.
      if transform.spec.urn == common_urns.primitives.FLATTEN.urn and len(
          transform.inputs) > 1:
        return False
      else:
        return transform.spec.urn not in context.runner_only_urns

    return all(is_sdk_transform(transform) for transform in self.transforms)

  def is_stateful(self):
    for transform in self.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if payload.state_specs or payload.timer_family_specs:
          return True
    return False

  def side_inputs(self):
    # type: () -> Iterator[str]
    for transform in self.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for side_input in payload.side_inputs:
          yield transform.inputs[side_input]

  def has_as_main_input(self, pcoll):
    for transform in self.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        local_side_inputs = payload.side_inputs
      else:
        local_side_inputs = {}
      for local_id, pipeline_id in transform.inputs.items():
        if pcoll == pipeline_id and local_id not in local_side_inputs:
          return True

  def deduplicate_read(self):
    # type: () -> None
    seen_pcolls = set()  # type: Set[str]
    new_transforms = []
    for transform in self.transforms:
      if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
        pcoll = only_element(list(transform.outputs.items()))[1]
        if pcoll in seen_pcolls:
          continue
        seen_pcolls.add(pcoll)
      new_transforms.append(transform)
    self.transforms = new_transforms

  def executable_stage_transform(
      self,
      known_runner_urns,  # type: FrozenSet[str]
      all_consumers,
      components  # type: beam_runner_api_pb2.Components
  ):
    # type: (...) -> beam_runner_api_pb2.PTransform
    if (len(self.transforms) == 1 and
        self.transforms[0].spec.urn in known_runner_urns):
      return self.transforms[0]

    else:
      all_inputs = set(
          pcoll for t in self.transforms for pcoll in t.inputs.values())
      all_outputs = set(
          pcoll for t in self.transforms for pcoll in t.outputs.values())
      internal_transforms = set(id(t) for t in self.transforms)
      external_outputs = [
          pcoll for pcoll in all_outputs
          if all_consumers[pcoll] - internal_transforms
      ]

      stage_components = beam_runner_api_pb2.Components()
      stage_components.CopyFrom(components)

      # Only keep the referenced PCollections.
      # Make pcollectionKey snapshot to avoid "Map modified during iteration"
      # in py3
      for pcoll_id in list(stage_components.pcollections.keys()):
        if pcoll_id not in all_inputs and pcoll_id not in all_outputs:
          del stage_components.pcollections[pcoll_id]

      # Only keep the transforms in this stage.
      # Also gather up payload data as we iterate over the transforms.
      stage_components.transforms.clear()
      main_inputs = set()  # type: Set[str]
      side_inputs = []
      user_states = []
      timers = []
      for ix, transform in enumerate(self.transforms):
        transform_id = 'transform_%d' % ix
        if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
          payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
          for tag in payload.side_inputs.keys():
            side_inputs.append(
                beam_runner_api_pb2.ExecutableStagePayload.SideInputId(
                    transform_id=transform_id, local_name=tag))
          for tag in payload.state_specs.keys():
            user_states.append(
                beam_runner_api_pb2.ExecutableStagePayload.UserStateId(
                    transform_id=transform_id, local_name=tag))
          for tag in payload.timer_family_specs.keys():
            timers.append(
                beam_runner_api_pb2.ExecutableStagePayload.TimerId(
                    transform_id=transform_id, local_name=tag))
          main_inputs.update(
              pcoll_id for tag,
              pcoll_id in transform.inputs.items()
              if tag not in payload.side_inputs)
        else:
          main_inputs.update(transform.inputs.values())
        stage_components.transforms[transform_id].CopyFrom(transform)

      main_input_id = only_element(main_inputs - all_outputs)
      named_inputs = dict({
          '%s:%s' % (side.transform_id, side.local_name):
          stage_components.transforms[side.transform_id].inputs[side.local_name]
          for side in side_inputs
      },
                          main_input=main_input_id)
      exec_payload = beam_runner_api_pb2.ExecutableStagePayload(
          environment=components.environments[self.environment],
          input=main_input_id,
          outputs=external_outputs,
          transforms=stage_components.transforms.keys(),
          components=stage_components,
          side_inputs=side_inputs,
          user_states=user_states,
          timers=timers)

      return beam_runner_api_pb2.PTransform(
          unique_name=unique_name(None, self.name),
          spec=beam_runner_api_pb2.FunctionSpec(
              urn='beam:runner:executable_stage:v1',
              payload=exec_payload.SerializeToString()),
          inputs=named_inputs,
          outputs={
              'output_%d' % ix: pcoll
              for ix,
              pcoll in enumerate(external_outputs)
          },
      )


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
      value.urn for (key, value) in common_urns.coders.__dict__.items()
      if not key.startswith('_')
      # Length prefix Rows rather than re-coding them.
  ) - set([common_urns.coders.ROW.urn])

  def __init__(
      self,
      components,  # type: beam_runner_api_pb2.Components
      known_runner_urns,  # type: FrozenSet[str]
      use_state_iterables=False,
      is_drain=False):
    self.components = components
    self.known_runner_urns = known_runner_urns
    self.runner_only_urns = known_runner_urns - frozenset(
        [common_urns.primitives.FLATTEN.urn])
    self.use_state_iterables = use_state_iterables
    self.is_drain = is_drain
    # ok to pass None for context because BytesCoder has no components
    coder_proto = coders.BytesCoder().to_runner_api(
        None)  # type: ignore[arg-type]
    self.bytes_coder_id = self.add_or_get_coder_id(coder_proto, 'bytes_coder')
    self.safe_coders = {self.bytes_coder_id: self.bytes_coder_id}
    self.data_channel_coders = {}

  def add_or_get_coder_id(
      self,
      coder_proto,  # type: beam_runner_api_pb2.Coder
      coder_prefix='coder'):
    # type: (...) -> str
    for coder_id, coder in self.components.coders.items():
      if coder == coder_proto:
        return coder_id
    new_coder_id = unique_name(self.components.coders, coder_prefix)
    self.components.coders[new_coder_id].CopyFrom(coder_proto)
    return new_coder_id

  def add_data_channel_coder(self, pcoll_id):
    pcoll = self.components.pcollections[pcoll_id]
    proto = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=common_urns.coders.WINDOWED_VALUE.urn),
        component_coder_ids=[
            pcoll.coder_id,
            self.components.windowing_strategies[
                pcoll.windowing_strategy_id].window_coder_id
        ])
    self.data_channel_coders[pcoll_id] = self.maybe_length_prefixed_coder(
        self.add_or_get_coder_id(proto, pcoll.coder_id + '_windowed'))

  @memoize_on_instance
  def with_state_iterables(self, coder_id):
    # type: (str) -> str
    coder = self.components.coders[coder_id]
    if coder.spec.urn == common_urns.coders.ITERABLE.urn:
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_state_backed')
      new_coder = self.components.coders[new_coder_id]
      new_coder.CopyFrom(coder)
      new_coder.spec.urn = common_urns.coders.STATE_BACKED_ITERABLE.urn
      new_coder.spec.payload = b'1'
      new_coder.component_coder_ids[0] = self.with_state_iterables(
          coder.component_coder_ids[0])
      return new_coder_id
    else:
      new_component_ids = [
          self.with_state_iterables(c) for c in coder.component_coder_ids
      ]
      if new_component_ids == coder.component_coder_ids:
        return coder_id
      else:
        new_coder_id = unique_name(
            self.components.coders, coder_id + '_state_backed')
        self.components.coders[new_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec, component_coder_ids=new_component_ids))
        return new_coder_id

  @memoize_on_instance
  def maybe_length_prefixed_coder(self, coder_id):
    # type: (str) -> str
    if coder_id in self.safe_coders:
      return coder_id
    (maybe_length_prefixed_id,
     safe_id) = self.maybe_length_prefixed_and_safe_coder(coder_id)
    self.safe_coders[maybe_length_prefixed_id] = safe_id
    return maybe_length_prefixed_id

  @memoize_on_instance
  def maybe_length_prefixed_and_safe_coder(self, coder_id):
    # type: (str) -> Tuple[str, str]
    coder = self.components.coders[coder_id]
    if coder.spec.urn == common_urns.coders.LENGTH_PREFIX.urn:
      return coder_id, self.bytes_coder_id
    elif coder.spec.urn in self._KNOWN_CODER_URNS:
      new_component_ids = [
          self.maybe_length_prefixed_coder(c) for c in coder.component_coder_ids
      ]
      if new_component_ids == coder.component_coder_ids:
        new_coder_id = coder_id
      else:
        new_coder_id = unique_name(
            self.components.coders, coder_id + '_length_prefixed')
        self.components.coders[new_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec, component_coder_ids=new_component_ids))
      safe_component_ids = [self.safe_coders[c] for c in new_component_ids]
      if safe_component_ids == coder.component_coder_ids:
        safe_coder_id = coder_id
      else:
        safe_coder_id = unique_name(self.components.coders, coder_id + '_safe')
        self.components.coders[safe_coder_id].CopyFrom(
            beam_runner_api_pb2.Coder(
                spec=coder.spec, component_coder_ids=safe_component_ids))
      return new_coder_id, safe_coder_id
    else:
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_length_prefixed')
      self.components.coders[new_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.LENGTH_PREFIX.urn),
              component_coder_ids=[coder_id]))
      return new_coder_id, self.bytes_coder_id

  def length_prefix_pcoll_coders(self, pcoll_id):
    # type: (str) -> None
    self.components.pcollections[pcoll_id].coder_id = (
        self.maybe_length_prefixed_coder(
            self.components.pcollections[pcoll_id].coder_id))


def leaf_transform_stages(
    root_ids, components, parent=None, known_composites=KNOWN_COMPOSITES):
  # type: (...) -> Iterator[Stage]
  for root_id in root_ids:
    root = components.transforms[root_id]
    if root.spec.urn in known_composites:
      yield Stage(root_id, [root], parent=parent)
    elif not root.subtransforms:
      # Make sure its outputs are not a subset of its inputs.
      if set(root.outputs.values()) - set(root.inputs.values()):
        yield Stage(root_id, [root], parent=parent)
    else:
      for stage in leaf_transform_stages(root.subtransforms,
                                         components,
                                         root_id,
                                         known_composites):
        yield stage


def pipeline_from_stages(
    pipeline_proto,  # type: beam_runner_api_pb2.Pipeline
    stages,  # type: Iterable[Stage]
    known_runner_urns,  # type: FrozenSet[str]
    partial  # type: bool
):
  # type: (...) -> beam_runner_api_pb2.Pipeline

  # In case it was a generator that mutates components as it
  # produces outputs (as is the case with most transformations).
  stages = list(stages)

  new_proto = beam_runner_api_pb2.Pipeline()
  new_proto.CopyFrom(pipeline_proto)
  components = new_proto.components
  components.transforms.clear()

  roots = set()
  parents = {
      child: parent
      for parent,
      proto in pipeline_proto.components.transforms.items()
      for child in proto.subtransforms
  }

  def add_parent(child, parent):
    if parent is None:
      roots.add(child)
    else:
      if isinstance(parent, Stage):
        parent = parent.name
      if (parent not in components.transforms and
          parent in pipeline_proto.components.transforms):
        components.transforms[parent].CopyFrom(
            pipeline_proto.components.transforms[parent])
        del components.transforms[parent].subtransforms[:]
        add_parent(parent, parents.get(parent))
      components.transforms[parent].subtransforms.append(child)

  def copy_subtransforms(transform):
    for subtransform_id in transform.subtransforms:
      if subtransform_id not in pipeline_proto.components.transforms:
        raise RuntimeError(
            'Could not find subtransform to copy: ' + subtransform_id)
      subtransform = pipeline_proto.components.transforms[subtransform_id]
      components.transforms[subtransform_id].CopyFrom(subtransform)
      copy_subtransforms(subtransform)

  all_consumers = collections.defaultdict(
      set)  # type: DefaultDict[str, Set[int]]
  for stage in stages:
    for transform in stage.transforms:
      for pcoll in transform.inputs.values():
        all_consumers[pcoll].add(id(transform))

  for stage in stages:
    if partial:
      transform = only_element(stage.transforms)
      copy_subtransforms(transform)
    else:
      transform = stage.executable_stage_transform(
          known_runner_urns, all_consumers, components)
    transform_id = unique_name(components.transforms, stage.name)
    components.transforms[transform_id].CopyFrom(transform)
    add_parent(transform_id, stage.parent)

  del new_proto.root_transform_ids[:]
  new_proto.root_transform_ids.extend(roots)

  return new_proto


def create_and_optimize_stages(
    pipeline_proto,  # type: beam_runner_api_pb2.Pipeline
    phases,
    known_runner_urns,  # type: FrozenSet[str]
    use_state_iterables=False,
    is_drain=False):
  # type: (...) -> Tuple[TransformContext, List[Stage]]

  """Create a set of stages given a pipeline proto, and set of optimizations.

  Args:
    pipeline_proto (beam_runner_api_pb2.Pipeline): A pipeline defined by a user.
    phases (callable): Each phase identifies a specific transformation to be
      applied to the pipeline graph. Existing phases are defined in this file,
      and receive a list of stages, and a pipeline context. Some available
      transformations are ``lift_combiners``, ``expand_sdf``, ``expand_gbk``,
      etc.

  Returns:
    A tuple with a pipeline context, and a list of stages (i.e. an optimized
    graph).
  """
  pipeline_context = TransformContext(
      pipeline_proto.components,
      known_runner_urns,
      use_state_iterables=use_state_iterables,
      is_drain=is_drain)

  # Initial set of stages are singleton leaf transforms.
  stages = list(
      leaf_transform_stages(
          pipeline_proto.root_transform_ids,
          pipeline_proto.components,
          union(known_runner_urns, KNOWN_COMPOSITES)))

  # Apply each phase in order.
  for phase in phases:
    _LOGGER.info('%s %s %s', '=' * 20, phase, '=' * 20)
    stages = list(phase(stages, pipeline_context))
    _LOGGER.debug('%s %s' % (len(stages), [len(s.transforms) for s in stages]))
    _LOGGER.debug('Stages: %s', [str(s) for s in stages])

  # Return the (possibly mutated) context and ordered set of stages.
  return pipeline_context, stages


def optimize_pipeline(
    pipeline_proto,  # type: beam_runner_api_pb2.Pipeline
    phases,
    known_runner_urns,  # type: FrozenSet[str]
    partial=False,
    **kwargs):
  unused_context, stages = create_and_optimize_stages(
      pipeline_proto,
      phases,
      known_runner_urns,
      **kwargs)
  return pipeline_from_stages(
      pipeline_proto, stages, known_runner_urns, partial)


# Optimization stages.


def annotate_downstream_side_inputs(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]

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
  consumers = collections.defaultdict(
      list)  # type: DefaultDict[str, List[Stage]]

  def get_all_side_inputs():
    # type: () -> Set[str]
    all_side_inputs = set()  # type: Set[str]
    for stage in stages:
      for transform in stage.transforms:
        for input in transform.inputs.values():
          consumers[input].append(stage)
      for si in stage.side_inputs():
        all_side_inputs.add(si)
    return all_side_inputs

  all_side_inputs = frozenset(get_all_side_inputs())

  downstream_side_inputs_by_stage = {}  # type: Dict[Stage, FrozenSet[str]]

  def compute_downstream_side_inputs(stage):
    # type: (Stage) -> FrozenSet[str]
    if stage not in downstream_side_inputs_by_stage:
      downstream_side_inputs = frozenset()  # type: FrozenSet[str]
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


def annotate_stateful_dofns_as_roots(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]
  for stage in stages:
    for transform in stage.transforms:
      if transform.spec.urn == common_urns.primitives.PAR_DO.urn:
        pardo_payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if pardo_payload.state_specs or pardo_payload.timer_family_specs:
          stage.forced_root = True
    yield stage


def fix_side_input_pcoll_coders(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]

  """Length prefix side input PCollection coders.
  """
  for stage in stages:
    for si in stage.side_inputs():
      pipeline_context.length_prefix_pcoll_coders(si)
  return stages


class _StagePacker(object):
  def get_group_key(self, stage, context):
    raise NotImplementedError()

  def pack_stages(self, group_key, grouped_stages, context):
    raise NotImplementedError()

  def process_stages(self, stages, context):
    stages = list(stages)
    stages_with_group_key = [(stage, self.get_group_key(stage, context))
                             for stage in stages]
    stages_by_group_key = collections.defaultdict(list)
    for stage, group_key in stages_with_group_key:
      if group_key is not None:
        stages_by_group_key[group_key].append(stage)
    packed_stages_by_group_key = {}
    pcoll_id_remap = {}
    for group_key, grouped_stages in stages_by_group_key.items():
      if len(grouped_stages) > 1:
        pack_stages_result = self.pack_stages(
            group_key, grouped_stages, context)
        if pack_stages_result is not None:
          packed_stages, group_pcoll_id_remap = pack_stages_result
          packed_stages_by_group_key[group_key] = packed_stages
          pcoll_id_remap.update(group_pcoll_id_remap)

    for transform in context.components.transforms.values():
      self._remap_input_pcolls(transform, pcoll_id_remap)

    group_keys_with_yielded_packed_stages = set()
    for stage, group_key in stages_with_group_key:
      if group_key is not None and group_key in packed_stages_by_group_key:
        if group_key not in group_keys_with_yielded_packed_stages:
          group_keys_with_yielded_packed_stages.add(group_key)
          for packed_stage in packed_stages_by_group_key[group_key]:
            self._remap_input_pcolls(
                only_transform(stage.transforms), pcoll_id_remap)
            yield packed_stage
      else:
        self._remap_input_pcolls(
            only_transform(stage.transforms), pcoll_id_remap)
        yield stage

  def _remap_input_pcolls(self, transform, pcoll_id_remap):
    for input_key in list(transform.inputs.keys()):
      if transform.inputs[input_key] in pcoll_id_remap:
        transform.inputs[input_key] = pcoll_id_remap[
            transform.inputs[input_key]]


class _EliminateCommonKeyWithNoneStagePacker(_StagePacker):
  def get_group_key(self, stage, context):
    if len(stage.transforms) == 1:
      transform = only_transform(stage.transforms)
      if (transform.spec.urn == common_urns.primitives.PAR_DO.urn and
          len(transform.inputs) == 1 and len(transform.outputs) == 1):
        pardo_payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if pardo_payload.do_fn.urn == python_urns.KEY_WITH_NONE_DOFN:
          return (only_element(transform.inputs.values()), stage.environment)
    return None

  def pack_stages(self, group_key, grouped_stages, context):
    first_pcoll_id = only_element(
        only_transform(grouped_stages[0].transforms).outputs.values())
    pcoll_ids_to_remap = [
        only_element(only_transform(stage.transforms).outputs.values())
        for stage in grouped_stages[1:]
    ]
    return (
        grouped_stages[:1],
        {pcoll_id: first_pcoll_id
         for pcoll_id in pcoll_ids_to_remap})


def eliminate_common_key_with_none(stages, context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]

  """Runs common subexpression elimination for sibling KeyWithNone stages.

  If multiple KeyWithNone stages share a common input, then all but one stages
  will be eliminated along with their output PCollections. Transforms that
  originally read input from the output PCollection of the eliminated
  KeyWithNone stages will be remapped to read input from the output PCollection
  of the remaining KeyWithNone stage.
  """
  packer = _EliminateCommonKeyWithNoneStagePacker()
  for stage in packer.process_stages(stages, context):
    yield stage


def _get_fallback_coder_id():
  return context.add_or_get_coder_id(
      coders.registry.get_coder(object).to_runner_api(None))


def _get_component_coder_id_from_kv_coder(coder, index):
  assert index < 2
  if coder.spec.urn == common_urns.coders.KV.urn and len(
      coder.component_coder_ids) == 2:
    return coder.component_coder_ids[index]
  return _get_fallback_coder_id()


def _get_key_coder_id_from_kv_coder(coder):
  return _get_component_coder_id_from_kv_coder(coder, 0)


def _get_value_coder_id_from_kv_coder(coder):
  return _get_component_coder_id_from_kv_coder(coder, 1)


def _try_fuse_stages(a, b):
  if a.can_fuse(b, context):
    return a.fuse(b)
  else:
    raise ValueError


class _PackCombinersStagePacker(_StagePacker):
  def get_group_key(self, stage, context):
    if (len(stage.transforms) == 1 and stage.environment is not None and
        python_urns.PACKED_COMBINE_FN
        in context.components.environments[stage.environment].capabilities):
      transform = only_transform(stage.transforms)
      if (transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn and
          len(transform.inputs) == 1 and len(transform.outputs) == 1):
        combine_payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.CombinePayload)
        if combine_payload.combine_fn.urn == python_urns.PICKLED_COMBINE_FN:
          return (only_element(transform.inputs.values()), stage.environment)
    return None

  def pack_stages(self, group_key, grouped_stages, context):
    class _UnpackFn(core.DoFn):
      """A DoFn that unpacks a packed to multiple tagged outputs.

      Example:
        tags = (T1, T2, ...)
        input = (K, (V1, V2, ...))
        output = TaggedOutput(T1, (K, V1)), TaggedOutput(T2, (K, V1)), ...
      """
      def __init__(self, tags):
        self._tags = tags

      def process(self, element):
        key, values = element
        return [
            core.pvalue.TaggedOutput(tag, (key, value)) for tag,
            value in zip(self._tags, values)
        ]

    input_pcoll_id, _ = group_key
    try:
      if not len(grouped_stages) > 1:
        raise ValueError('Only one stage in this group: Skipping stage packing')
      # Fused stage is used as template and is not yielded.
      fused_stage = functools.reduce(_try_fuse_stages, grouped_stages)
    except ValueError:
      # Skip packing stages in this group.
      return None

    transforms = [only_transform(stage.transforms) for stage in grouped_stages]
    combine_payloads = [
        proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.CombinePayload)
        for transform in transforms
    ]
    output_pcoll_ids = [
        only_element(transform.outputs.values()) for transform in transforms
    ]

    # Build accumulator coder for (acc1, acc2, ...)
    accumulator_coder_ids = [
        combine_payload.accumulator_coder_id
        for combine_payload in combine_payloads
    ]
    tuple_accumulator_coder_id = context.add_or_get_coder_id(
        beam_runner_api_pb2.Coder(
            spec=beam_runner_api_pb2.FunctionSpec(urn=python_urns.TUPLE_CODER),
            component_coder_ids=accumulator_coder_ids))

    # Build packed output coder for (key, (out1, out2, ...))
    input_kv_coder_id = context.components.pcollections[input_pcoll_id].coder_id
    key_coder_id = _get_key_coder_id_from_kv_coder(
        context.components.coders[input_kv_coder_id])
    output_kv_coder_ids = [
        context.components.pcollections[output_pcoll_id].coder_id
        for output_pcoll_id in output_pcoll_ids
    ]
    output_value_coder_ids = [
        _get_value_coder_id_from_kv_coder(
            context.components.coders[output_kv_coder_id])
        for output_kv_coder_id in output_kv_coder_ids
    ]
    pack_output_value_coder = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(urn=python_urns.TUPLE_CODER),
        component_coder_ids=output_value_coder_ids)
    pack_output_value_coder_id = context.add_or_get_coder_id(
        pack_output_value_coder)
    pack_output_kv_coder = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(urn=common_urns.coders.KV.urn),
        component_coder_ids=[key_coder_id, pack_output_value_coder_id])
    pack_output_kv_coder_id = context.add_or_get_coder_id(pack_output_kv_coder)

    # Set up packed PCollection
    pack_combine_name = fused_stage.name
    pack_pcoll_id = unique_name(context.components.pcollections, 'pcollection')
    input_pcoll = context.components.pcollections[input_pcoll_id]
    context.components.pcollections[pack_pcoll_id].CopyFrom(
        beam_runner_api_pb2.PCollection(
            unique_name=pack_combine_name + '.out',
            coder_id=pack_output_kv_coder_id,
            windowing_strategy_id=input_pcoll.windowing_strategy_id,
            is_bounded=input_pcoll.is_bounded))

    # Use the parent of the first stage as the parent stage for the Pack and
    # Unpack stages. This ensures that the Pack and Unpack transform nodes are
    # placed after all transforms that produce its input PCollections and before
    # all transforms that consume its output PCollections.
    stage_parent = grouped_stages[0].parent

    # Set up Pack stage.
    pack_combine_fn = combiners.SingleInputTupleCombineFn(
        *[
            core.CombineFn.from_runner_api(combine_payload.combine_fn, context)
            for combine_payload in combine_payloads
        ]).to_runner_api(context)
    pack_transform = beam_runner_api_pb2.PTransform(
        unique_name=pack_combine_name + '/Pack',
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=common_urns.composites.COMBINE_PER_KEY.urn,
            payload=beam_runner_api_pb2.CombinePayload(
                combine_fn=pack_combine_fn,
                accumulator_coder_id=tuple_accumulator_coder_id).
            SerializeToString()),
        inputs={'in': input_pcoll_id},
        outputs={'out': pack_pcoll_id},
        environment_id=fused_stage.environment)
    pack_stage = Stage(
        pack_combine_name + '/Pack', [pack_transform],
        downstream_side_inputs=fused_stage.downstream_side_inputs,
        must_follow=fused_stage.must_follow,
        parent=stage_parent,
        environment=fused_stage.environment)

    # Set up Unpack stage
    tags = [str(i) for i in range(len(output_pcoll_ids))]
    pickled_do_fn_data = pickler.dumps((_UnpackFn(tags), (), {}, [], None))
    unpack_transform = beam_runner_api_pb2.PTransform(
        unique_name=pack_combine_name + '/Unpack',
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=common_urns.primitives.PAR_DO.urn,
            payload=beam_runner_api_pb2.ParDoPayload(
                do_fn=beam_runner_api_pb2.FunctionSpec(
                    urn=python_urns.PICKLED_DOFN_INFO,
                    payload=pickled_do_fn_data)).SerializeToString()),
        inputs={'in': pack_pcoll_id},
        outputs=dict(zip(tags, output_pcoll_ids)),
        environment_id=fused_stage.environment)
    unpack_stage = Stage(
        pack_combine_name + '/Unpack', [unpack_transform],
        downstream_side_inputs=fused_stage.downstream_side_inputs,
        must_follow=fused_stage.must_follow,
        parent=stage_parent,
        environment=fused_stage.environment)
    return ([pack_stage, unpack_stage], {})


def pack_combiners(stages, context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Packs sibling CombinePerKey stages into a single CombinePerKey.

  If CombinePerKey stages have a common input, one input each, and one output
  each, pack the stages into a single stage that runs all CombinePerKeys and
  outputs resulting tuples to a new PCollection. A subsequent stage unpacks
  tuples from this PCollection and sends them to the original output
  PCollections.
  """
  packer = _PackCombinersStagePacker()
  for stage in packer.process_stages(stages, context):
    yield stage


def lift_combiners(stages, context):
  # type: (List[Stage], TransformContext) -> Iterator[Stage]

  """Expands CombinePerKey into pre- and post-grouping stages.

  ... -> CombinePerKey -> ...

  becomes

  ... -> PreCombine -> GBK -> MergeAccumulators -> ExtractOutput -> ...
  """
  def is_compatible_with_combiner_lifting(trigger):
    '''Returns whether this trigger is compatible with combiner lifting.

    Certain triggers, such as those that fire after a certain number of
    elements, need to observe every element, and as such are incompatible
    with combiner lifting (which may aggregate several elements into one
    before they reach the triggering code after shuffle).
    '''
    if trigger is None:
      return True
    elif trigger.WhichOneof('trigger') in (
        'default',
        'always',
        'never',
        'after_processing_time',
        'after_synchronized_processing_time'):
      return True
    elif trigger.HasField('element_count'):
      return trigger.element_count.element_count == 1
    elif trigger.HasField('after_end_of_window'):
      return is_compatible_with_combiner_lifting(
          trigger.after_end_of_window.early_firings
      ) and is_compatible_with_combiner_lifting(
          trigger.after_end_of_window.late_firings)
    elif trigger.HasField('after_any'):
      return all(
          is_compatible_with_combiner_lifting(t)
          for t in trigger.after_any.subtriggers)
    elif trigger.HasField('repeat'):
      return is_compatible_with_combiner_lifting(trigger.repeat.subtrigger)
    else:
      return False

  def can_lift(combine_per_key_transform):
    windowing = context.components.windowing_strategies[
        context.components.pcollections[only_element(
            list(combine_per_key_transform.inputs.values())
        )].windowing_strategy_id]
    return is_compatible_with_combiner_lifting(windowing.trigger)

  def make_stage(base_stage, transform):
    # type: (Stage, beam_runner_api_pb2.PTransform) -> Stage
    return Stage(
        transform.unique_name, [transform],
        downstream_side_inputs=base_stage.downstream_side_inputs,
        must_follow=base_stage.must_follow,
        parent=base_stage,
        environment=base_stage.environment)

  def lifted_stages(stage):
    transform = stage.transforms[0]
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
        spec=beam_runner_api_pb2.FunctionSpec(urn=common_urns.coders.KV.urn),
        component_coder_ids=[key_coder_id, accumulator_coder_id])
    key_accumulator_coder_id = context.add_or_get_coder_id(
        key_accumulator_coder)

    accumulator_iter_coder = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=common_urns.coders.ITERABLE.urn),
        component_coder_ids=[accumulator_coder_id])
    accumulator_iter_coder_id = context.add_or_get_coder_id(
        accumulator_iter_coder)

    key_accumulator_iter_coder = beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(urn=common_urns.coders.KV.urn),
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

    yield make_stage(
        stage,
        beam_runner_api_pb2.PTransform(
            unique_name=transform.unique_name + '/Precombine',
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=common_urns.combine_components.COMBINE_PER_KEY_PRECOMBINE.
                urn,
                payload=transform.spec.payload),
            inputs=transform.inputs,
            outputs={'out': precombined_pcoll_id},
            environment_id=transform.environment_id))

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
                urn=common_urns.combine_components.
                COMBINE_PER_KEY_MERGE_ACCUMULATORS.urn,
                payload=transform.spec.payload),
            inputs={'in': grouped_pcoll_id},
            outputs={'out': merged_pcoll_id},
            environment_id=transform.environment_id))

    yield make_stage(
        stage,
        beam_runner_api_pb2.PTransform(
            unique_name=transform.unique_name + '/ExtractOutputs',
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=common_urns.combine_components.
                COMBINE_PER_KEY_EXTRACT_OUTPUTS.urn,
                payload=transform.spec.payload),
            inputs={'in': merged_pcoll_id},
            outputs=transform.outputs,
            environment_id=transform.environment_id))

  def unlifted_stages(stage):
    transform = stage.transforms[0]
    for sub in transform.subtransforms:
      yield make_stage(stage, context.components.transforms[sub])

  for stage in stages:
    transform = only_transform(stage.transforms)
    if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
      expansion = lifted_stages if can_lift(transform) else unlifted_stages
      for substage in expansion(stage):
        yield substage
    else:
      yield stage


def expand_sdf(stages, context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Transforms splitable DoFns into pair+split+read."""
  for stage in stages:
    transform = only_transform(stage.transforms)
    if transform.spec.urn == common_urns.primitives.PAR_DO.urn:

      pardo_payload = proto_utils.parse_Bytes(
          transform.spec.payload, beam_runner_api_pb2.ParDoPayload)

      if pardo_payload.restriction_coder_id:

        def copy_like(protos, original, suffix='_copy', **kwargs):
          if isinstance(original, (str, unicode)):
            key = original
            original = protos[original]
          else:
            key = 'component'
          new_id = unique_name(protos, key + suffix)
          protos[new_id].CopyFrom(original)
          proto = protos[new_id]
          for name, value in kwargs.items():
            if isinstance(value, dict):
              getattr(proto, name).clear()
              getattr(proto, name).update(value)
            elif isinstance(value, list):
              del getattr(proto, name)[:]
              getattr(proto, name).extend(value)
            elif name == 'urn':
              proto.spec.urn = value
            elif name == 'payload':
              proto.spec.payload = value
            else:
              setattr(proto, name, value)
          if 'unique_name' not in kwargs and hasattr(proto, 'unique_name'):
            proto.unique_name = unique_name(
                {p.unique_name
                 for p in protos.values()},
                original.unique_name + suffix)
          return new_id

        def make_stage(base_stage, transform_id, extra_must_follow=()):
          # type: (Stage, str, Iterable[Stage]) -> Stage
          transform = context.components.transforms[transform_id]
          return Stage(
              transform.unique_name, [transform],
              base_stage.downstream_side_inputs,
              union(base_stage.must_follow, frozenset(extra_must_follow)),
              parent=base_stage,
              environment=base_stage.environment)

        main_input_tag = only_element(
            tag for tag in transform.inputs.keys()
            if tag not in pardo_payload.side_inputs)
        main_input_id = transform.inputs[main_input_tag]
        element_coder_id = context.components.pcollections[
            main_input_id].coder_id
        # Tuple[element, restriction]
        paired_coder_id = context.add_or_get_coder_id(
            beam_runner_api_pb2.Coder(
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=common_urns.coders.KV.urn),
                component_coder_ids=[
                    element_coder_id, pardo_payload.restriction_coder_id
                ]))
        # Tuple[Tuple[element, restriction], double]
        sized_coder_id = context.add_or_get_coder_id(
            beam_runner_api_pb2.Coder(
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=common_urns.coders.KV.urn),
                component_coder_ids=[
                    paired_coder_id,
                    context.add_or_get_coder_id(
                        # context can be None here only because FloatCoder does
                        # not have components
                        coders.FloatCoder().to_runner_api(None),  # type: ignore
                        'doubles_coder')
                ]))

        paired_pcoll_id = copy_like(
            context.components.pcollections,
            main_input_id,
            '_paired',
            coder_id=paired_coder_id)
        pair_transform_id = copy_like(
            context.components.transforms,
            transform,
            unique_name=transform.unique_name + '/PairWithRestriction',
            urn=common_urns.sdf_components.PAIR_WITH_RESTRICTION.urn,
            outputs={'out': paired_pcoll_id})

        split_pcoll_id = copy_like(
            context.components.pcollections,
            main_input_id,
            '_split',
            coder_id=sized_coder_id)
        split_transform_id = copy_like(
            context.components.transforms,
            transform,
            unique_name=transform.unique_name + '/SplitAndSizeRestriction',
            urn=common_urns.sdf_components.SPLIT_AND_SIZE_RESTRICTIONS.urn,
            inputs=dict(transform.inputs, **{main_input_tag: paired_pcoll_id}),
            outputs={'out': split_pcoll_id})

        reshuffle_stage = None
        if common_urns.composites.RESHUFFLE.urn in context.known_runner_urns:
          reshuffle_pcoll_id = copy_like(
              context.components.pcollections,
              main_input_id,
              '_reshuffle',
              coder_id=sized_coder_id)
          reshuffle_transform_id = copy_like(
              context.components.transforms,
              transform,
              unique_name=transform.unique_name + '/Reshuffle',
              urn=common_urns.composites.RESHUFFLE.urn,
              payload=b'',
              inputs=dict(transform.inputs, **{main_input_tag: split_pcoll_id}),
              outputs={'out': reshuffle_pcoll_id})
          reshuffle_stage = make_stage(stage, reshuffle_transform_id)
        else:
          reshuffle_pcoll_id = split_pcoll_id
          reshuffle_transform_id = None

        if context.is_drain:
          truncate_pcoll_id = copy_like(
              context.components.pcollections,
              main_input_id,
              '_truncate_restriction',
              coder_id=sized_coder_id)
          # Lengthprefix the truncate output.
          context.length_prefix_pcoll_coders(truncate_pcoll_id)
          truncate_transform_id = copy_like(
              context.components.transforms,
              transform,
              unique_name=transform.unique_name + '/TruncateAndSizeRestriction',
              urn=common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn,
              inputs=dict(
                  transform.inputs, **{main_input_tag: reshuffle_pcoll_id}),
              outputs={'out': truncate_pcoll_id})
          process_transform_id = copy_like(
              context.components.transforms,
              transform,
              unique_name=transform.unique_name + '/Process',
              urn=common_urns.sdf_components.
              PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS.urn,
              inputs=dict(
                  transform.inputs, **{main_input_tag: truncate_pcoll_id}))
        else:
          process_transform_id = copy_like(
              context.components.transforms,
              transform,
              unique_name=transform.unique_name + '/Process',
              urn=common_urns.sdf_components.
              PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS.urn,
              inputs=dict(
                  transform.inputs, **{main_input_tag: reshuffle_pcoll_id}))

        yield make_stage(stage, pair_transform_id)
        split_stage = make_stage(stage, split_transform_id)
        yield split_stage
        if reshuffle_stage:
          yield reshuffle_stage
        if context.is_drain:
          yield make_stage(
              stage, truncate_transform_id, extra_must_follow=[split_stage])
          yield make_stage(stage, process_transform_id)
        else:
          yield make_stage(
              stage, process_transform_id, extra_must_follow=[split_stage])

      else:
        yield stage

    else:
      yield stage


def expand_gbk(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Transforms each GBK into a write followed by a read."""
  for stage in stages:
    transform = only_transform(stage.transforms)
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
          [
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Write',
                  inputs=transform.inputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_OUTPUT_URN,
                      payload=grouping_buffer))
          ],
          downstream_side_inputs=frozenset(),
          must_follow=stage.must_follow)
      yield gbk_write

      yield Stage(
          transform.unique_name + '/Read',
          [
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Read',
                  outputs=transform.outputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN,
                      payload=grouping_buffer))
          ],
          downstream_side_inputs=stage.downstream_side_inputs,
          must_follow=union(frozenset([gbk_write]), stage.must_follow))
    else:
      yield stage


def fix_flatten_coders(
    stages, pipeline_context, identity_urn=bundle_processor.IDENTITY_DOFN_URN):
  # type: (Iterable[Stage], TransformContext, str) -> Iterator[Stage]

  """Ensures that the inputs of Flatten have the same coders as the output.
  """
  pcollections = pipeline_context.components.pcollections
  for stage in stages:
    transform = only_element(stage.transforms)
    if transform.spec.urn == common_urns.primitives.FLATTEN.urn:
      output_pcoll_id = only_element(transform.outputs.values())
      output_coder_id = pcollections[output_pcoll_id].coder_id
      for local_in, pcoll_in in list(transform.inputs.items()):
        if pcollections[pcoll_in].coder_id != output_coder_id:
          # Flatten requires that all its inputs be materialized with the
          # same coder as its output.  Add stages to transcode flatten
          # inputs that use different coders.
          transcoded_pcollection = unique_name(
              pcollections,
              transform.unique_name + '/Transcode/' + local_in + '/out')
          transcode_name = unique_name(
              pipeline_context.components.transforms,
              transform.unique_name + '/Transcode/' + local_in)
          yield Stage(
              transcode_name,
              [
                  beam_runner_api_pb2.PTransform(
                      unique_name=transcode_name,
                      inputs={local_in: pcoll_in},
                      outputs={'out': transcoded_pcollection},
                      spec=beam_runner_api_pb2.FunctionSpec(urn=identity_urn),
                      environment_id=transform.environment_id)
              ],
              downstream_side_inputs=frozenset(),
              must_follow=stage.must_follow)
          pcollections[transcoded_pcollection].CopyFrom(pcollections[pcoll_in])
          pcollections[transcoded_pcollection].unique_name = (
              transcoded_pcollection)
          pcollections[transcoded_pcollection].coder_id = output_coder_id
          transform.inputs[local_in] = transcoded_pcollection

    yield stage


def sink_flattens(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Sink flattens and remove them from the graph.

  A flatten that cannot be sunk/fused away becomes multiple writes (to the
  same logical sink) followed by a read.
  """
  # TODO(robertwb): Actually attempt to sink rather than always materialize.
  # TODO(robertwb): Possibly fuse multi-input flattens into one of the stages.
  for stage in fix_flatten_coders(stages,
                                  pipeline_context,
                                  common_urns.primitives.FLATTEN.urn):
    transform = only_element(stage.transforms)
    if (transform.spec.urn == common_urns.primitives.FLATTEN.urn and
        len(transform.inputs) > 1):
      # This is used later to correlate the read and writes.
      buffer_id = create_buffer_id(transform.unique_name)
      flatten_writes = []  # type: List[Stage]
      for local_in, pcoll_in in transform.inputs.items():
        flatten_write = Stage(
            transform.unique_name + '/Write/' + local_in,
            [
                beam_runner_api_pb2.PTransform(
                    unique_name=transform.unique_name + '/Write/' + local_in,
                    inputs={local_in: pcoll_in},
                    spec=beam_runner_api_pb2.FunctionSpec(
                        urn=bundle_processor.DATA_OUTPUT_URN,
                        payload=buffer_id),
                    environment_id=transform.environment_id)
            ],
            downstream_side_inputs=frozenset(),
            must_follow=stage.must_follow)
        flatten_writes.append(flatten_write)
        yield flatten_write

      yield Stage(
          transform.unique_name + '/Read',
          [
              beam_runner_api_pb2.PTransform(
                  unique_name=transform.unique_name + '/Read',
                  outputs=transform.outputs,
                  spec=beam_runner_api_pb2.FunctionSpec(
                      urn=bundle_processor.DATA_INPUT_URN, payload=buffer_id),
                  environment_id=transform.environment_id)
          ],
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

  # Now try to fuse away all pcollections.
  for pcoll, producer in producers_by_pcoll.items():
    write_pcoll = None
    for consumer in consumers_by_pcoll[pcoll]:
      producer = replacement(producer)
      consumer = replacement(consumer)
      # Update consumer.must_follow set, as it's used in can_fuse.
      consumer.must_follow = frozenset(
          replacement(s) for s in consumer.must_follow)
      if producer.can_fuse(consumer, pipeline_context):
        fuse(producer, consumer)
      else:
        # If we can't fuse, do a read + write.
        pipeline_context.length_prefix_pcoll_coders(pcoll)
        buffer_id = create_buffer_id(pcoll)
        if write_pcoll is None:
          write_pcoll = Stage(
              pcoll + '/Write',
              [
                  beam_runner_api_pb2.PTransform(
                      unique_name=pcoll + '/Write',
                      inputs={'in': pcoll},
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=bundle_processor.DATA_OUTPUT_URN,
                          payload=buffer_id))
              ],
              downstream_side_inputs=producer.downstream_side_inputs)
          fuse(producer, write_pcoll)
        if consumer.has_as_main_input(pcoll):
          read_pcoll = Stage(
              pcoll + '/Read',
              [
                  beam_runner_api_pb2.PTransform(
                      unique_name=pcoll + '/Read',
                      outputs={'out': pcoll},
                      spec=beam_runner_api_pb2.FunctionSpec(
                          urn=bundle_processor.DATA_INPUT_URN,
                          payload=buffer_id))
              ],
              downstream_side_inputs=consumer.downstream_side_inputs,
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
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

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
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

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


def extract_impulse_stages(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Splits fused Impulse operations into their own stage."""
  for stage in stages:
    for transform in list(stage.transforms):
      if transform.spec.urn == common_urns.primitives.IMPULSE.urn:
        stage.transforms.remove(transform)
        yield Stage(
            transform.unique_name,
            transforms=[transform],
            downstream_side_inputs=stage.downstream_side_inputs,
            must_follow=stage.must_follow,
            parent=stage.parent)

    if stage.transforms:
      yield stage


def remove_data_plane_ops(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]
  for stage in stages:
    for transform in list(stage.transforms):
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        stage.transforms.remove(transform)

    if stage.transforms:
      yield stage


def setup_timer_mapping(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """Set up a mapping of {transform_id: [timer_ids]} for each stage.
  """
  for stage in stages:
    for transform in stage.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        for timer_family_id in payload.timer_family_specs.keys():
          stage.timers.add((transform.unique_name, timer_family_id))
    yield stage


def sort_stages(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> List[Stage]

  """Order stages suitable for sequential execution.
  """
  all_stages = set(stages)
  seen = set()  # type: Set[Stage]
  ordered = []

  def process(stage):
    if stage not in seen:
      seen.add(stage)
      if stage not in all_stages:
        return
      for prev in stage.must_follow:
        process(prev)
      ordered.append(stage)

  for stage in stages:
    process(stage)
  return ordered


def populate_data_channel_coders(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]

  """Populate coders for GRPC input and output ports."""
  for stage in stages:
    for transform in stage.transforms:
      if transform.spec.urn in (bundle_processor.DATA_INPUT_URN,
                                bundle_processor.DATA_OUTPUT_URN):
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          sdk_pcoll_id = only_element(transform.outputs.values())
        else:
          sdk_pcoll_id = only_element(transform.inputs.values())
        pipeline_context.add_data_channel_coder(sdk_pcoll_id)

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
  # type: (Optional[Container[str]], str) -> str
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
  # type: (Iterable[T]) -> T
  element, = iterable
  return element


def only_transform(transforms):
  # type: (List[beam_runner_api_pb2.PTransform]) -> beam_runner_api_pb2.PTransform
  assert len(transforms) == 1
  return transforms[0]


def create_buffer_id(name, kind='materialize'):
  # type: (str, str) -> bytes
  return ('%s:%s' % (kind, name)).encode('utf-8')


def split_buffer_id(buffer_id):
  # type: (bytes) -> Tuple[str, str]

  """A buffer id is "kind:pcollection_id". Split into (kind, pcoll_id). """
  kind, pcoll_id = buffer_id.decode('utf-8').split(':', 1)
  return kind, pcoll_id
