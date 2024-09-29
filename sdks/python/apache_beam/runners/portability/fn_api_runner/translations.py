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
# mypy: check-untyped-defs

import collections
import copy
import functools
import itertools
import logging
import operator
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Collection
from typing import Container
from typing import DefaultDict
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import Iterator
from typing import List
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeVar
from typing import Union

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.worker import bundle_processor
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp

if TYPE_CHECKING:
  from apache_beam.runners.portability.fn_api_runner.execution import ListBuffer
  from apache_beam.runners.portability.fn_api_runner.execution import PartitionableBuffer

T = TypeVar('T')

# This module is experimental. No backwards-compatibility guarantees.

_LOGGER = logging.getLogger(__name__)

KNOWN_COMPOSITES = frozenset([
    common_urns.primitives.GROUP_BY_KEY.urn,
    common_urns.composites.COMBINE_PER_KEY.urn,
    common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
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

# TimerFamilyId is identified by transform name + timer family
# TODO(pabloem): Rename this type to express this name is unique per pipeline.
TimerFamilyId = Tuple[str, str]

BufferId = bytes

# SideInputId is identified by a consumer ParDo + tag.
SideInputId = Tuple[str, str]
SideInputAccessPattern = beam_runner_api_pb2.FunctionSpec

# A map from a PCollection coder ID to a Safe Coder ID
# A safe coder is a coder that can be used on the runner-side of the FnApi.
# A safe coder receives a byte string, and returns a type that can be
# understood by the runner when deserializing.
SafeCoderMapping = Dict[str, str]

# DataSideInput maps SideInputIds to a tuple of the encoded bytes of the side
# input content, and a payload specification regarding the type of side input
# (MultiMap / Iterable).
DataSideInput = Dict[SideInputId, Tuple[bytes, SideInputAccessPattern]]

DataOutput = Dict[str, BufferId]

# A map of [Transform ID, Timer Family ID] to [Buffer ID, Time Domain for timer]
# The time domain comes from beam_runner_api_pb2.TimeDomain. It may be
# EVENT_TIME or PROCESSING_TIME.
OutputTimers = MutableMapping[TimerFamilyId, Tuple[BufferId, Any]]

# A map of [Transform ID, Timer Family ID] to [Buffer CONTENTS, Timestamp]
OutputTimerData = MutableMapping[TimerFamilyId,
                                 Tuple['PartitionableBuffer',
                                       timestamp.Timestamp]]

BundleProcessResult = Tuple[beam_fn_api_pb2.InstructionResponse,
                            List[beam_fn_api_pb2.ProcessBundleSplitResponse]]


# TODO(pabloem): Change tha name to a more representative one
class DataInput(NamedTuple):
  data: MutableMapping[str, 'PartitionableBuffer']
  timers: MutableMapping[TimerFamilyId, 'PartitionableBuffer']


class Stage(object):
  """A set of Transforms that can be sent to the worker for processing."""
  def __init__(
      self,
      name,  # type: str
      transforms,  # type: List[beam_runner_api_pb2.PTransform]
      downstream_side_inputs=None,  # type: Optional[FrozenSet[str]]
      must_follow=frozenset(),  # type: FrozenSet[Stage]
      parent=None,  # type: Optional[str]
      environment=None,  # type: Optional[str]
      forced_root=False):
    self.name = name
    self.transforms = transforms
    self.downstream_side_inputs = downstream_side_inputs
    self.must_follow = must_follow
    self.timers = set()  # type: Set[TimerFamilyId]
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

  def fuse(self, other, context):
    # type: (Stage, TransformContext) -> Stage
    return Stage(
        "(%s)+(%s)" % (self.name, other.name),
        self.transforms + other.transforms,
        union(self.downstream_side_inputs, other.downstream_side_inputs),
        union(self.must_follow, other.must_follow),
        environment=self._merge_environments(
            self.environment, other.environment),
        parent=_parent_for_fused_stages([self, other], context),
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
      yield from side_inputs(transform).values()

  def has_as_main_input(self, pcoll):
    for transform in self.transforms:
      if transform.spec.urn in PAR_DO_URNS:
        payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        local_side_inputs = payload.side_inputs
      else:
        local_side_inputs = {}  # type: ignore[assignment]
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
      result = copy.copy(self.transforms[0])
      del result.subtransforms[:]
      return result

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

      # Only keep the PCollections referenced in this stage.
      stage_components.pcollections.clear()
      for pcoll_id in all_inputs.union(all_outputs):
        stage_components.pcollections[pcoll_id].CopyFrom(
            components.pcollections[pcoll_id])

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
      # at this point we should have resolved an environment, as the key of
      # components.environments cannot be None.
      assert self.environment is not None
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
              urn=common_urns.executable_stage,
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

  _COMMON_CODER_URNS = set(
      value.urn for (key, value) in common_urns.coders.__dict__.items()
      if not key.startswith('_')
      # Length prefix Rows rather than re-coding them.
  ) - set([common_urns.coders.ROW.urn])

  _REQUIRED_CODER_URNS = set([
      common_urns.coders.WINDOWED_VALUE.urn,
      # For impulse.
      common_urns.coders.BYTES.urn,
      common_urns.coders.GLOBAL_WINDOW.urn,
      # For GBK.
      common_urns.coders.KV.urn,
      common_urns.coders.ITERABLE.urn,
      # For SDF.
      common_urns.coders.DOUBLE.urn,
      # For timers.
      common_urns.coders.TIMER.urn,
      # For everything else.
      common_urns.coders.LENGTH_PREFIX.urn,
      common_urns.coders.CUSTOM_WINDOW.urn,
  ])

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
    self._known_coder_urns = set.union(
        # Those which are required.
        self._REQUIRED_CODER_URNS,
        # Those common coders which are understood by many environments.
        self._COMMON_CODER_URNS.intersection(
            *(
                set(env.capabilities)
                for env in self.components.environments.values())))
    self.use_state_iterables = use_state_iterables
    self.is_drain = is_drain
    # ok to pass None for context because BytesCoder has no components
    coder_proto = coders.BytesCoder().to_runner_api(
        None)  # type: ignore[arg-type]
    self.bytes_coder_id = self.add_or_get_coder_id(coder_proto, 'bytes_coder')

    self.safe_coders: SafeCoderMapping = {
        self.bytes_coder_id: self.bytes_coder_id
    }

    # A map of PCollection ID to Coder ID.
    self.data_channel_coders = {}  # type: Dict[str, str]

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
      # If the coder is already length prefixed, we can use it as is, and
      # have the runner treat it as opaque bytes.
      return coder_id, self.bytes_coder_id
    elif (coder.spec.urn == common_urns.coders.WINDOWED_VALUE.urn and
          self.components.coders[coder.component_coder_ids[1]].spec.urn not in
          self._known_coder_urns):
      # A WindowedValue coder with an unknown window type.
      # This needs to be encoded in such a way that we still have access to its
      # timestmap.
      lp_elem_coder = self.maybe_length_prefixed_coder(
          coder.component_coder_ids[0])
      tp_window_coder = self.timestamped_prefixed_window_coder(
          coder.component_coder_ids[1])
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_timestamp_prefixed')
      self.components.coders[new_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.WINDOWED_VALUE.urn),
              component_coder_ids=[lp_elem_coder, tp_window_coder]))
      safe_coder_id = unique_name(
          self.components.coders, coder_id + '_timestamp_prefixed_opaque')
      self.components.coders[safe_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.WINDOWED_VALUE.urn),
              component_coder_ids=[
                  self.safe_coders[lp_elem_coder],
                  self.safe_coders[tp_window_coder]
              ]))
      return new_coder_id, safe_coder_id
    elif coder.spec.urn in self._known_coder_urns:
      # A known coder type, but its components may still need to be length
      # prefixed.
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
      # A completely unkown coder. Wrap the entire thing in a length prefix.
      new_coder_id = unique_name(
          self.components.coders, coder_id + '_length_prefixed')
      self.components.coders[new_coder_id].CopyFrom(
          beam_runner_api_pb2.Coder(
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.coders.LENGTH_PREFIX.urn),
              component_coder_ids=[coder_id]))
      return new_coder_id, self.bytes_coder_id

  @memoize_on_instance
  def timestamped_prefixed_window_coder(self, coder_id):
    length_prefixed = self.maybe_length_prefixed_coder(coder_id)
    new_coder_id = unique_name(
        self.components.coders, coder_id + '_timestamp_prefixed')
    self.components.coders[new_coder_id].CopyFrom(
        beam_runner_api_pb2.Coder(
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=common_urns.coders.CUSTOM_WINDOW.urn),
            component_coder_ids=[length_prefixed]))
    safe_coder_id = unique_name(
        self.components.coders, coder_id + '_timestamp_prefixed_opaque')
    self.components.coders[safe_coder_id].CopyFrom(
        beam_runner_api_pb2.Coder(
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=python_urns.TIMESTAMP_PREFIXED_OPAQUE_WINDOW_CODER)))
    self.safe_coders[new_coder_id] = safe_coder_id
    return new_coder_id

  def length_prefix_pcoll_coders(self, pcoll_id):
    # type: (str) -> None
    self.components.pcollections[pcoll_id].coder_id = (
        self.maybe_length_prefixed_coder(
            self.components.pcollections[pcoll_id].coder_id))

  @memoize_on_instance
  def parents_map(self):
    return {
        child: parent
        for (parent, transform) in self.components.transforms.items()
        for child in transform.subtransforms
    }



def leaf_transform_stages(
    root_ids,  # type: Iterable[str]
    components,  # type: beam_runner_api_pb2.Components
    parent=None,  # type: Optional[str]
    known_composites=KNOWN_COMPOSITES  # type: FrozenSet[str]
):
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
  components.pcollections.clear()

  # order preserving but still has fast contains checking
  roots = {}  # type: Dict[str, Any]
  parents = {
      child: parent
      for parent,
      proto in pipeline_proto.components.transforms.items()
      for child in proto.subtransforms
  }

  def copy_output_pcollections(transform):
    for pcoll_id in transform.outputs.values():
      components.pcollections[pcoll_id].CopyFrom(
          pipeline_proto.components.pcollections[pcoll_id])

  def add_parent(child, parent):
    if parent is None:
      if child not in roots:
        roots[child] = None
    else:
      if (parent not in components.transforms and
          parent in pipeline_proto.components.transforms):
        components.transforms[parent].CopyFrom(
            pipeline_proto.components.transforms[parent])
        copy_output_pcollections(components.transforms[parent])
        del components.transforms[parent].subtransforms[:]
      # Ensure that child is the last item in the parent's subtransforms.
      # If the stages were previously sorted into topological order using
      # sort_stages, this ensures that the parent transforms are also
      # added in topological order.
      if child in components.transforms[parent].subtransforms:
        components.transforms[parent].subtransforms.remove(child)
      components.transforms[parent].subtransforms.append(child)
      add_parent(parent, parents.get(parent))

  def copy_subtransforms(transform):
    for subtransform_id in transform.subtransforms:
      if subtransform_id not in pipeline_proto.components.transforms:
        raise RuntimeError(
            'Could not find subtransform to copy: ' + subtransform_id)
      subtransform = pipeline_proto.components.transforms[subtransform_id]
      components.transforms[subtransform_id].CopyFrom(subtransform)
      copy_output_pcollections(components.transforms[subtransform_id])
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
          known_runner_urns, all_consumers, pipeline_proto.components)
    transform_id = unique_name(components.transforms, stage.name)
    components.transforms[transform_id].CopyFrom(transform)
    copy_output_pcollections(transform)
    add_parent(transform_id, stage.parent)

  del new_proto.root_transform_ids[:]
  new_proto.root_transform_ids.extend(roots.keys())

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
          known_composites=union(known_runner_urns, KNOWN_COMPOSITES)))

  # Apply each phase in order.
  for phase in phases:
    stages = list(phase(stages, pipeline_context))

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


def standard_optimize_phases():
  """Returns the basic set of phases, to be passed to optimize_pipeline,
  that result in a pipeline consisting only of fused stages (with urn
  beam:runner:executable_stage:v1) and, of course, those designated as known
  runner urns, in topological order.
  """
  return [
      annotate_downstream_side_inputs,
      annotate_stateful_dofns_as_roots,
      fix_side_input_pcoll_coders,
      pack_combiners,
      lift_combiners,
      expand_sdf,
      fix_flatten_coders,
      # sink_flattens,
      greedily_fuse,
      read_to_impulse,
      extract_impulse_stages,
      remove_data_plane_ops,
      sort_stages,
  ]


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


def _group_stages_by_key(stages, get_stage_key):
  grouped_stages = collections.defaultdict(list)
  stages_with_none_key = []
  for stage in stages:
    stage_key = get_stage_key(stage)
    if stage_key is None:
      stages_with_none_key.append(stage)
    else:
      grouped_stages[stage_key].append(stage)
  return (grouped_stages, stages_with_none_key)


def _group_stages_with_limit(stages, get_limit):
  # type: (Iterable[Stage], Callable[[str], int]) -> Iterable[Collection[Stage]]
  stages_with_limit = [(stage, get_limit(stage.name)) for stage in stages]
  group: List[Stage] = []
  group_limit = 0
  for stage, limit in sorted(stages_with_limit, key=operator.itemgetter(1)):
    if limit < 1:
      raise Exception(
          'expected get_limit to return an integer >= 1, '
          'instead got: %d for stage: %s' % (limit, stage))
    if not group:
      group_limit = limit
    assert len(group) < group_limit
    group.append(stage)
    if len(group) >= group_limit:
      yield group
      group = []
  if group:
    yield group


def _remap_input_pcolls(transform, pcoll_id_remap):
  for input_key in list(transform.inputs.keys()):
    if transform.inputs[input_key] in pcoll_id_remap:
      transform.inputs[input_key] = pcoll_id_remap[transform.inputs[input_key]]


def _make_pack_name(names):
  """Return the packed Transform or Stage name.

  The output name will contain the input names' common prefix, the infix
  '/Packed', and the input names' suffixes in square brackets.
  For example, if the input names are 'a/b/c1/d1' and 'a/b/c2/d2, then
  the output name is 'a/b/Packed[c1_d1, c2_d2]'.
  """
  assert names
  tokens_in_names = [name.split('/') for name in names]
  common_prefix_tokens = []

  # Find the longest common prefix of tokens.
  while True:
    first_token_in_names = set()
    for tokens in tokens_in_names:
      if not tokens:
        break
      first_token_in_names.add(tokens[0])
    if len(first_token_in_names) != 1:
      break
    common_prefix_tokens.append(next(iter(first_token_in_names)))
    for tokens in tokens_in_names:
      tokens.pop(0)

  common_prefix_tokens.append('Packed')
  common_prefix = '/'.join(common_prefix_tokens)
  suffixes = ['_'.join(tokens) for tokens in tokens_in_names]
  return '%s[%s]' % (common_prefix, ', '.join(suffixes))


def _eliminate_common_key_with_none(stages, context, can_pack=lambda s: True):
  # type: (Iterable[Stage], TransformContext, Callable[[str], Union[bool, int]]) -> Iterable[Stage]

  """Runs common subexpression elimination for sibling KeyWithNone stages.

  If multiple KeyWithNone stages share a common input, then all but one stages
  will be eliminated along with their output PCollections. Transforms that
  originally read input from the output PCollection of the eliminated
  KeyWithNone stages will be remapped to read input from the output PCollection
  of the remaining KeyWithNone stage.
  """

  # Partition stages by whether they are eligible for common KeyWithNone
  # elimination, and group eligible KeyWithNone stages by parent and
  # environment.
  def get_stage_key(stage):
    if len(stage.transforms) == 1 and can_pack(stage.name):
      transform = only_transform(stage.transforms)
      if (transform.spec.urn == common_urns.primitives.PAR_DO.urn and
          len(transform.inputs) == 1 and len(transform.outputs) == 1):
        pardo_payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
        if pardo_payload.do_fn.urn == python_urns.KEY_WITH_NONE_DOFN:
          return (only_element(transform.inputs.values()), stage.environment)
    return None

  grouped_eligible_stages, ineligible_stages = _group_stages_by_key(
      stages, get_stage_key)

  # Eliminate stages and build the PCollection remapping dictionary.
  pcoll_id_remap = {}
  remaining_stages = []
  for sibling_stages in grouped_eligible_stages.values():
    if len(sibling_stages) > 1:
      output_pcoll_ids = [
          only_element(stage.transforms[0].outputs.values())
          for stage in sibling_stages
      ]
      parent = _parent_for_fused_stages(sibling_stages, context)
      for to_delete_pcoll_id in output_pcoll_ids[1:]:
        pcoll_id_remap[to_delete_pcoll_id] = output_pcoll_ids[0]
        del context.components.pcollections[to_delete_pcoll_id]
      sibling_stages[0].parent = parent
      sibling_stages[0].name = _make_pack_name(
          stage.name for stage in sibling_stages)
      only_transform(
          sibling_stages[0].transforms).unique_name = _make_pack_name(
              only_transform(stage.transforms).unique_name
              for stage in sibling_stages)

    remaining_stages.append(sibling_stages[0])

  # Remap all transforms in components.
  for transform in context.components.transforms.values():
    _remap_input_pcolls(transform, pcoll_id_remap)

  # Yield stages while remapping input PCollections if needed.
  stages_to_yield = itertools.chain(ineligible_stages, remaining_stages)
  for stage in stages_to_yield:
    transform = only_transform(stage.transforms)
    _remap_input_pcolls(transform, pcoll_id_remap)
    yield stage


_DEFAULT_PACK_COMBINERS_LIMIT = 128


def replace_gbk_combinevalue_pairs(stages, context):
  # type: (Iterable[Stage], TransformContext) -> Iterator[Stage]

  """
  Replaces GroupByKey + CombineValues pairs into CombinePerKey.
  """
  processed_stages_by_name = set()
  for stage in stages:
    transform = only_element(stage.transforms)
    if transform.unique_name in processed_stages_by_name:
      continue
    if transform.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
      consumer_transforms = [
          context.components.transforms[consumer]
          for consumer in transform.outputs.values()
      ]
      if not all(consumer.spec.urn ==
                 common_urns.combine_components.COMBINE_GROUPED_VALUES.urn
                 for consumer in consumer_transforms):
        yield stage
      for consumer in consumer_transforms:
        # Replace GroupByKey + CombineValues with CombinePerKey.
        # The name of the new merged stage is the GBK stage name joined with
        # the CombineValues stage name, e.g. "GBK+CombineValues"
        def label(transform):
          if transform.unique_name == '':
            return ''
          try:
            return transform.unique_name.rsplit('/', 1)[1]
          except IndexError:
            return transform.unique_name

        name = '%s+%s' % (label(transform), label(consumer))
        unique_name = consumer.unique_name.rsplit('/', 1)[0] + '/' + name
        stage = Stage(
            unique_name,
            [
                beam_runner_api_pb2.PTransform(
                    unique_name=unique_name,
                    inputs={'input': only_element(transform.inputs.values())},
                    spec=beam_runner_api_pb2.FunctionSpec(
                        urn=common_urns.composites.COMBINE_PER_KEY.urn),
                    environment_id=transform.environment_id)
            ],
            downstream_side_inputs=frozenset(),
            must_follow=stage.must_follow)
        stage.transforms[0].outputs.MergeFrom(consumer.outputs)
        processed_stages_by_name.add(consumer.unique_name)
        yield stage
      processed_stages_by_name.add(transform.unique_name)
    else:
      yield stage


def pack_per_key_combiners(stages, context, can_pack=lambda s: True):
  # type: (Iterable[Stage], TransformContext, Callable[[str], Union[bool, int]]) -> Iterator[Stage]

  """Packs sibling CombinePerKey stages into a single CombinePerKey.

  If CombinePerKey stages have a common input, one input each, and one output
  each, pack the stages into a single stage that runs all CombinePerKeys and
  outputs resulting tuples to a new PCollection. A subsequent stage unpacks
  tuples from this PCollection and sends them to the original output
  PCollections.
  """
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

  def _get_fallback_coder_id():
    return context.add_or_get_coder_id(
        # passing None works here because there are no component coders
        coders.registry.get_coder(object).to_runner_api(None))  # type: ignore[arg-type]

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
      return a.fuse(b, context)
    else:
      raise ValueError

  def _get_limit(stage_name):
    result = can_pack(stage_name)
    if result is True:
      return _DEFAULT_PACK_COMBINERS_LIMIT
    else:
      return int(result)

  # Partition stages by whether they are eligible for CombinePerKey packing
  # and group eligible CombinePerKey stages by parent and environment.
  def get_stage_key(stage):
    if (len(stage.transforms) == 1 and can_pack(stage.name) and
        stage.environment is not None and python_urns.PACKED_COMBINE_FN in
        context.components.environments[stage.environment].capabilities):
      transform = only_transform(stage.transforms)
      if (transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn and
          len(transform.inputs) == 1 and len(transform.outputs) == 1):
        combine_payload = proto_utils.parse_Bytes(
            transform.spec.payload, beam_runner_api_pb2.CombinePayload)
        if combine_payload.combine_fn.urn == python_urns.PICKLED_COMBINE_FN:
          return (only_element(transform.inputs.values()), stage.environment)
    return None

  grouped_eligible_stages, ineligible_stages = _group_stages_by_key(
      stages, get_stage_key)
  for stage in ineligible_stages:
    yield stage

  grouped_packable_stages = [(stage_key, subgrouped_stages) for stage_key,
                             grouped_stages in grouped_eligible_stages.items()
                             for subgrouped_stages in _group_stages_with_limit(
                                 grouped_stages, _get_limit)]

  for stage_key, packable_stages in grouped_packable_stages:
    input_pcoll_id, _ = stage_key
    try:
      if not len(packable_stages) > 1:
        raise ValueError('Only one stage in this group: Skipping stage packing')
      # Fused stage is used as template and is not yielded.
      fused_stage = functools.reduce(_try_fuse_stages, packable_stages)
    except ValueError:
      # Skip packing stages in this group.
      # Yield the stages unmodified, and then continue to the next group.
      for stage in packable_stages:
        yield stage
      continue

    transforms = [only_transform(stage.transforms) for stage in packable_stages]
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

    pack_stage_name = _make_pack_name([stage.name for stage in packable_stages])
    pack_transform_name = _make_pack_name([
        only_transform(stage.transforms).unique_name
        for stage in packable_stages
    ])
    pack_pcoll_id = unique_name(context.components.pcollections, 'pcollection')
    input_pcoll = context.components.pcollections[input_pcoll_id]
    context.components.pcollections[pack_pcoll_id].CopyFrom(
        beam_runner_api_pb2.PCollection(
            unique_name=pack_transform_name + '/Pack.out',
            coder_id=pack_output_kv_coder_id,
            windowing_strategy_id=input_pcoll.windowing_strategy_id,
            is_bounded=input_pcoll.is_bounded))

    # Set up Pack stage.
    # TODO(https://github.com/apache/beam/issues/19737): classes that inherit
    #  from RunnerApiFn are expected to accept a PipelineContext for
    #  from_runner_api/to_runner_api.  Determine how to accomodate this.
    pack_combine_fn = combiners.SingleInputTupleCombineFn(
        *[
            core.CombineFn.from_runner_api(combine_payload.combine_fn, context)  # type: ignore[arg-type]
            for combine_payload in combine_payloads
        ]).to_runner_api(context)  # type: ignore[arg-type]
    pack_transform = beam_runner_api_pb2.PTransform(
        unique_name=pack_transform_name + '/Pack',
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=common_urns.composites.COMBINE_PER_KEY.urn,
            payload=beam_runner_api_pb2.CombinePayload(
                combine_fn=pack_combine_fn,
                accumulator_coder_id=tuple_accumulator_coder_id).
            SerializeToString()),
        inputs={'in': input_pcoll_id},
        # 'None' single output key follows convention for CombinePerKey.
        outputs={'None': pack_pcoll_id},
        environment_id=fused_stage.environment)
    pack_stage = Stage(
        pack_stage_name + '/Pack', [pack_transform],
        downstream_side_inputs=fused_stage.downstream_side_inputs,
        must_follow=fused_stage.must_follow,
        parent=fused_stage.parent,
        environment=fused_stage.environment)
    yield pack_stage

    # Set up Unpack stage
    tags = [str(i) for i in range(len(output_pcoll_ids))]
    pickled_do_fn_data = pickler.dumps((_UnpackFn(tags), (), {}, [], None))
    unpack_transform = beam_runner_api_pb2.PTransform(
        unique_name=pack_transform_name + '/Unpack',
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
        pack_stage_name + '/Unpack', [unpack_transform],
        downstream_side_inputs=fused_stage.downstream_side_inputs,
        must_follow=fused_stage.must_follow,
        parent=fused_stage.parent,
        environment=fused_stage.environment)
    yield unpack_stage


def pack_combiners(stages, context, can_pack=None):
  # type: (Iterable[Stage], TransformContext, Optional[Callable[[str], Union[bool, int]]]) -> Iterator[Stage]
  if can_pack is None:
    can_pack_names = {}  # type: Dict[str, Union[bool, int]]
    parents = context.parents_map()

    def can_pack_fn(name: str) -> Union[bool, int]:
      if name in can_pack_names:
        return can_pack_names[name]
      else:
        transform = context.components.transforms[name]
        if python_urns.APPLY_COMBINER_PACKING in transform.annotations:
          try:
            result = int(
                transform.annotations[python_urns.APPLY_COMBINER_PACKING])
          except ValueError:
            result = True
        elif name in parents:
          result = can_pack_fn(parents[name])
        else:
          result = False
        can_pack_names[name] = result
        return result

    can_pack = can_pack_fn

  yield from pack_per_key_combiners(
      _eliminate_common_key_with_none(stages, context, can_pack),
      context,
      can_pack)


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
        parent=base_stage.name,
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
            annotations=transform.annotations,
            environment_id=transform.environment_id))

    yield make_stage(
        stage,
        beam_runner_api_pb2.PTransform(
            unique_name=transform.unique_name + '/Group',
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=common_urns.primitives.GROUP_BY_KEY.urn),
            inputs={'in': precombined_pcoll_id},
            annotations=transform.annotations,
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
            annotations=transform.annotations,
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
            annotations=transform.annotations,
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


def _lowest_common_ancestor(a, b, parents):
  # type: (str, str, Dict[str, str]) -> Optional[str]

  '''Returns the name of the lowest common ancestor of the two named stages.

  The map of stage names to their parents' stage names should be provided
  in parents. Note that stages are considered to be ancestors of themselves.
  '''
  assert a != b

  def get_ancestors(name):
    ancestor = name
    while ancestor is not None:
      yield ancestor
      ancestor = parents.get(ancestor)

  a_ancestors = set(get_ancestors(a))
  for b_ancestor in get_ancestors(b):
    if b_ancestor in a_ancestors:
      return b_ancestor
  return None


def _parent_for_fused_stages(stages, context):
  # type: (Iterable[Stage], TransformContext) -> Optional[str]

  '''Returns the name of the new parent for the fused stages.

  The new parent is the lowest common ancestor of the fused stages that is not
  contained in the set of stages to be fused. The provided context is used to
  compute ancestors of stages.
  '''

  parents = context.parents_map()
  # If any of the input stages were produced by fusion or an optimizer phase,
  # or had its parent modified by an optimizer phase, its parent will not be
  # be reflected in the PipelineContext yet, so we need to add it to the
  # parents map.
  for stage in stages:
    parents[stage.name] = stage.parent

  def reduce_fn(a, b):
    # type: (Optional[str], Optional[str]) -> Optional[str]
    if a is None or b is None:
      return None
    return _lowest_common_ancestor(a, b, parents)

  stage_names = [stage.name for stage in stages]  # type: List[Optional[str]]
  result = functools.reduce(reduce_fn, stage_names)
  if result in stage_names:
    result = parents.get(result)
  return result


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
          if isinstance(original, str):
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
              parent=base_stage.name,
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
  # type: (Iterable[Stage], TransformContext) -> FrozenSet[Stage]

  """Places transforms sharing an edge in the same stage, whenever possible.
  """
  producers_by_pcoll = {}  # type: Dict[str, Stage]
  consumers_by_pcoll = collections.defaultdict(
      list)  # type: DefaultDict[str, List[Stage]]

  # Used to always reference the correct stage as the producer and
  # consumer maps are not updated when stages are fused away.
  replacements = {}  # type: Dict[Stage, Stage]

  def replacement(s):
    old_ss = []
    while s in replacements:
      old_ss.append(s)
      s = replacements[s]
    for old_s in old_ss[:-1]:
      replacements[old_s] = s
    return s

  def fuse(producer, consumer):
    fused = producer.fuse(consumer, pipeline_context)
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

  producers = {
      pcoll: stage
      for stage in all_stages for t in stage.transforms
      for pcoll in t.outputs.values()
  }

  def process(stage):
    if stage not in seen:
      seen.add(stage)
      if stage not in all_stages:
        return
      for prev in stage.must_follow:
        process(prev)
      stage_outputs = set(
          pcoll for transform in stage.transforms
          for pcoll in transform.outputs.values())
      for transform in stage.transforms:
        for pcoll in transform.inputs.values():
          if pcoll not in stage_outputs:
            process(producers[pcoll])
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


def add_impulse_to_dangling_transforms(stages, pipeline_context):
  # type: (Iterable[Stage], TransformContext) -> Iterable[Stage]

  """Populate coders for GRPC input and output ports."""
  for stage in stages:
    for transform in stage.transforms:
      if len(transform.inputs
             ) == 0 and transform.spec.urn != bundle_processor.DATA_INPUT_URN:
        # We look through the various stages in the DAG, and transforms. If we
        # see a transform that has no inputs whatsoever.
        impulse_pc = unique_name(
            pipeline_context.components.pcollections, 'Impulse')
        output_pcoll = pipeline_context.components.pcollections[next(
            iter(transform.outputs.values()))]
        pipeline_context.components.pcollections[impulse_pc].CopyFrom(
            beam_runner_api_pb2.PCollection(
                unique_name=impulse_pc,
                coder_id=pipeline_context.bytes_coder_id,
                windowing_strategy_id=output_pcoll.windowing_strategy_id,
                is_bounded=output_pcoll.is_bounded))
        transform.inputs['in'] = impulse_pc

        stage.transforms.append(
            beam_runner_api_pb2.PTransform(
                unique_name=transform.unique_name,
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=bundle_processor.DATA_INPUT_URN,
                    payload=IMPULSE_BUFFER),
                outputs={'out': impulse_pc}))
    yield stage


def union(a, b):
  # Minimize the number of distinct sets.
  if not a or a == b:
    return b
  elif not b:
    return a
  else:
    return frozenset.union(a, b)


_global_counter = 0


def side_inputs(transform):
  result = {}
  if transform.spec.urn in PAR_DO_URNS:
    payload = proto_utils.parse_Bytes(
        transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
    for side_input in payload.side_inputs:
      result[side_input] = transform.inputs[side_input]
  return result


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
