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

"""Utility class for serializing pipelines via the runner API.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file
# mypy: disallow-untyped-defs

from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Generic
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

from google.protobuf import message
from typing_extensions import Protocol

from apache_beam import coders
from apache_beam import pipeline
from apache_beam import pvalue
from apache_beam.coders.coder_impl import IterableStateReader
from apache_beam.coders.coder_impl import IterableStateWriter
from apache_beam.internal import pickler
from apache_beam.pipeline import ComponentIdMap
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import core
from apache_beam.transforms import environments
from apache_beam.transforms.resources import merge_resource_hints
from apache_beam.typehints import native_type_compatibility

PortableObjectT = TypeVar('PortableObjectT', bound='PortableObject')


class PortableObject(Protocol):
  def to_runner_api(self, __context: 'PipelineContext') -> Any:
    pass

  @classmethod
  def from_runner_api(cls, __proto: Any, __context: 'PipelineContext') -> Any:
    pass


class _PipelineContextMap(Generic[PortableObjectT]):
  """This is a bi-directional map between objects and ids.

  Under the hood it encodes and decodes these objects into runner API
  representations.
  """
  def __init__(
      self,
      context: 'PipelineContext',
      obj_type: Type[PortableObjectT],
      namespace: str,
      proto_map: Optional[Mapping[str, message.Message]] = None) -> None:
    self._pipeline_context = context
    self._obj_type = obj_type
    self._namespace = namespace
    self._obj_to_id: Dict[Any, str] = {}
    self._id_to_obj: Dict[str, Any] = {}
    self._id_to_proto = dict(proto_map) if proto_map else {}

  def populate_map(self, proto_map: Mapping[str, message.Message]) -> None:
    for id, proto in self._id_to_proto.items():
      proto_map[id].CopyFrom(proto)

  def get_id(self, obj: PortableObjectT, label: Optional[str] = None) -> str:
    if obj not in self._obj_to_id:
      id = self._pipeline_context.component_id_map.get_or_assign(
          obj, self._obj_type, label)
      self._id_to_obj[id] = obj
      self._obj_to_id[obj] = id
      self._id_to_proto[id] = obj.to_runner_api(self._pipeline_context)
    return self._obj_to_id[obj]

  def get_proto(
      self,
      obj: PortableObjectT,
      label: Optional[str] = None) -> message.Message:
    return self._id_to_proto[self.get_id(obj, label)]

  def get_by_id(self, id: str) -> PortableObjectT:
    if id not in self._id_to_obj:
      self._id_to_obj[id] = self._obj_type.from_runner_api(
          self._id_to_proto[id], self._pipeline_context)
    return self._id_to_obj[id]

  def get_by_proto(
      self,
      maybe_new_proto: message.Message,
      label: Optional[str] = None,
      deduplicate: bool = False) -> str:
    # TODO: this method may not be safe for arbitrary protos due to
    #  xlang concerns, hence limiting usage to the only current use-case it has.
    #  See: https://github.com/apache/beam/pull/14390#discussion_r616062377
    assert isinstance(maybe_new_proto, beam_runner_api_pb2.Environment)
    obj = self._obj_type.from_runner_api(
        maybe_new_proto, self._pipeline_context)

    if deduplicate:
      if obj in self._obj_to_id:
        return self._obj_to_id[obj]

      for id, proto in self._id_to_proto.items():
        if proto == maybe_new_proto:
          return id
    return self.put_proto(
        self._pipeline_context.component_id_map.get_or_assign(
            obj=obj, obj_type=self._obj_type, label=label),
        maybe_new_proto)

  def get_id_to_proto_map(self) -> Dict[str, message.Message]:
    return self._id_to_proto

  def get_proto_from_id(self, id: str) -> message.Message:
    return self.get_id_to_proto_map()[id]

  def put_proto(
      self,
      id: str,
      proto: message.Message,
      ignore_duplicates: bool = False) -> str:
    if not ignore_duplicates and id in self._id_to_proto:
      raise ValueError("Id '%s' is already taken." % id)
    elif (ignore_duplicates and id in self._id_to_proto and
          self._id_to_proto[id] != proto):
      raise ValueError(
          'Cannot insert different protos %r and %r with the same ID %r',
          self._id_to_proto[id],
          proto,
          id)
    self._id_to_proto[id] = proto
    return id

  def __getitem__(self, id: str) -> Any:
    return self.get_by_id(id)

  def __contains__(self, id: str) -> bool:
    return id in self._id_to_proto


class PipelineContext(object):
  """For internal use only; no backwards-compatibility guarantees.

  Used for accessing and constructing the referenced objects of a Pipeline.
  """
  def __init__(
      self,
      proto: Optional[Union[beam_runner_api_pb2.Components,
                            beam_fn_api_pb2.ProcessBundleDescriptor]] = None,
      component_id_map: Optional[pipeline.ComponentIdMap] = None,
      default_environment: Optional[environments.Environment] = None,
      use_fake_coders: bool = False,
      iterable_state_read: Optional[IterableStateReader] = None,
      iterable_state_write: Optional[IterableStateWriter] = None,
      namespace: str = 'ref',
      requirements: Iterable[str] = (),
  ) -> None:
    if isinstance(proto, beam_fn_api_pb2.ProcessBundleDescriptor):
      proto = beam_runner_api_pb2.Components(
          coders=dict(proto.coders.items()),
          windowing_strategies=dict(proto.windowing_strategies.items()),
          environments=dict(proto.environments.items()))

    self.component_id_map = component_id_map or ComponentIdMap(namespace)
    assert self.component_id_map.namespace == namespace

    # TODO(https://github.com/apache/beam/issues/20827) Initialize
    # component_id_map with objects from proto.
    self.transforms = _PipelineContextMap(
        self,
        pipeline.AppliedPTransform,
        namespace,
        proto.transforms if proto is not None else None)
    self.pcollections = _PipelineContextMap(
        self,
        pvalue.PCollection,
        namespace,
        proto.pcollections if proto is not None else None)
    self.coders = _PipelineContextMap(
        self,
        coders.Coder,
        namespace,
        proto.coders if proto is not None else None)
    self.windowing_strategies = _PipelineContextMap(
        self,
        core.Windowing,
        namespace,
        proto.windowing_strategies if proto is not None else None)
    self.environments = _PipelineContextMap(
        self,
        environments.Environment,
        namespace,
        proto.environments if proto is not None else None)

    if default_environment is None:
      default_environment = environments.DefaultEnvironment()

    self._default_environment_id: str = self.environments.get_id(
        default_environment, label='default_environment')

    self.use_fake_coders = use_fake_coders
    self.deterministic_coder_map: Mapping[coders.Coder, coders.Coder] = {}
    self.iterable_state_read = iterable_state_read
    self.iterable_state_write = iterable_state_write
    self._requirements = set(requirements)

  def add_requirement(self, requirement: str) -> None:
    self._requirements.add(requirement)

  def requirements(self) -> FrozenSet[str]:
    return frozenset(self._requirements)

  # If fake coders are requested, return a pickled version of the element type
  # rather than an actual coder. The element type is required for some runners,
  # as well as performing a round-trip through protos.
  # TODO(https://github.com/apache/beam/issues/18490): Remove once this is no
  # longer needed.
  def coder_id_from_element_type(
      self,
      element_type: Any,
      requires_deterministic_key_coder: Optional[str] = None) -> str:
    if self.use_fake_coders:
      return pickler.dumps(element_type).decode('ascii')
    else:
      coder = coders.registry.get_coder(element_type)
      if requires_deterministic_key_coder:
        coder = coders.TupleCoder([
            self.deterministic_coder(
                coder.key_coder(), requires_deterministic_key_coder),
            coder.value_coder()
        ])
      return self.coders.get_id(coder)

  def deterministic_coder(self, coder: coders.Coder, msg: str) -> coders.Coder:
    if coder not in self.deterministic_coder_map:
      self.deterministic_coder_map[coder] = coder.as_deterministic_coder(msg)  # type: ignore
    return self.deterministic_coder_map[coder]

  def element_type_from_coder_id(self, coder_id: str) -> Any:
    if self.use_fake_coders or coder_id not in self.coders:
      return pickler.loads(coder_id)
    else:
      return native_type_compatibility.convert_to_beam_type(
          self.coders[coder_id].to_type_hint())

  @staticmethod
  def from_runner_api(
      proto: beam_runner_api_pb2.Components) -> 'PipelineContext':
    return PipelineContext(proto)

  def to_runner_api(self) -> beam_runner_api_pb2.Components:
    context_proto = beam_runner_api_pb2.Components()

    self.transforms.populate_map(context_proto.transforms)
    self.pcollections.populate_map(context_proto.pcollections)
    self.coders.populate_map(context_proto.coders)
    self.windowing_strategies.populate_map(context_proto.windowing_strategies)
    self.environments.populate_map(context_proto.environments)

    return context_proto

  def default_environment_id(self) -> str:
    return self._default_environment_id

  def get_environment_id_for_resource_hints(
      self, hints: Dict[str, bytes]) -> str:
    """Returns an environment id that has necessary resource hints."""
    if not hints:
      return self.default_environment_id()

    def get_or_create_environment_with_resource_hints(
        template_env_id: str,
        resource_hints: Dict[str, bytes],
    ) -> str:
      """Creates an environment that has necessary hints and returns its id."""
      template_env = self.environments.get_proto_from_id(template_env_id)
      cloned_env = beam_runner_api_pb2.Environment()
      # (TODO https://github.com/apache/beam/issues/25615)
      # Remove the suppress warning for type once mypy is updated to 0.941 or
      # higher.
      #  mypy 0.790 throws the warning below but 0.941 doesn't.
      #  error: Argument 1 to "CopyFrom" of "Message" has incompatible type
      #  "Message"; expected "Environment"  [arg-type]
      # Here, Environment is a subclass of Message but mypy still
      # throws an error.
      cloned_env.CopyFrom(template_env)  # type: ignore[arg-type]
      cloned_env.resource_hints.clear()
      cloned_env.resource_hints.update(resource_hints)

      return self.environments.get_by_proto(
          cloned_env, label='environment_with_resource_hints', deduplicate=True)

    default_env_id = self.default_environment_id()
    env_hints = self.environments.get_by_id(default_env_id).resource_hints()
    hints = merge_resource_hints(outer_hints=env_hints, inner_hints=hints)
    maybe_new_env_id = get_or_create_environment_with_resource_hints(
        default_env_id, hints)

    return maybe_new_env_id
