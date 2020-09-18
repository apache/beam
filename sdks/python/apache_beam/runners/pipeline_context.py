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

from __future__ import absolute_import

from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Union

from apache_beam import coders
from apache_beam import pipeline
from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.pipeline import ComponentIdMap
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import core
from apache_beam.transforms import environments
from apache_beam.typehints import native_type_compatibility

if TYPE_CHECKING:
  from google.protobuf import message  # pylint: disable=ungrouped-imports
  from apache_beam.coders.coder_impl import IterableStateReader
  from apache_beam.coders.coder_impl import IterableStateWriter


class _PipelineContextMap(object):
  """This is a bi-directional map between objects and ids.

  Under the hood it encodes and decodes these objects into runner API
  representations.
  """
  def __init__(self,
               context,
               obj_type,
               namespace: str,
               proto_map: Optional[Mapping[str, message.Message]] = None
              ):
    self._pipeline_context = context
    self._obj_type = obj_type
    self._namespace = namespace
    self._obj_to_id: Dict[Any, str] = {}
    self._id_to_obj: Dict[str, Any] = {}
    self._id_to_proto = dict(proto_map) if proto_map else {}

  def populate_map(self, proto_map: Mapping[str, message.Message]) -> None:
    for id, proto in self._id_to_proto.items():
      proto_map[id].CopyFrom(proto)

  def get_id(self, obj: Any, label: Optional[str] = None) -> str:
    if obj not in self._obj_to_id:
      id = self._pipeline_context.component_id_map.get_or_assign(
          obj, self._obj_type, label)
      self._id_to_obj[id] = obj
      self._obj_to_id[obj] = id
      self._id_to_proto[id] = obj.to_runner_api(self._pipeline_context)
    return self._obj_to_id[obj]

  def get_proto(self, obj: Any, label: Optional[str] = None) -> message.Message:
    return self._id_to_proto[self.get_id(obj, label)]

  def get_by_id(self, id: str) -> Any:
    if id not in self._id_to_obj:
      self._id_to_obj[id] = self._obj_type.from_runner_api(
          self._id_to_proto[id], self._pipeline_context)
    return self._id_to_obj[id]

  def get_by_proto(self, maybe_new_proto: message.Message, label: Optional[str] = None, deduplicate: bool = False) -> str:
    if deduplicate:
      for id, proto in self._id_to_proto.items():
        if proto == maybe_new_proto:
          return id
    return self.put_proto(
        self._pipeline_context.component_id_map.get_or_assign(
            label, obj_type=self._obj_type),
        maybe_new_proto)

  def get_id_to_proto_map(self) -> Dict[str, message.Message]:
    return self._id_to_proto

  def get_proto_from_id(self, id):
    return self.get_id_to_proto_map()[id]

  def put_proto(self, id: str, proto: message.Message, ignore_duplicates: bool = False) -> str:
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

  def __init__(self,
               proto: Optional[Union[beam_runner_api_pb2.Components, beam_fn_api_pb2.ProcessBundleDescriptor]] = None,
               component_id_map: Optional[pipeline.ComponentIdMap] = None,
               default_environment: Optional[environments.Environment] = None,
               use_fake_coders=False,
               iterable_state_read: Optional[IterableStateReader] = None,
               iterable_state_write: Optional[IterableStateWriter] = None,
               namespace='ref',
               allow_proto_holders=False,
               requirements: Iterable[str] = (),
              ):
    if isinstance(proto, beam_fn_api_pb2.ProcessBundleDescriptor):
      proto = beam_runner_api_pb2.Components(
          coders=dict(proto.coders.items()),
          windowing_strategies=dict(proto.windowing_strategies.items()),
          environments=dict(proto.environments.items()))

    self.component_id_map = component_id_map or ComponentIdMap(namespace)
    assert self.component_id_map.namespace == namespace

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

    if default_environment:
      self._default_environment_id: Optional[str] = self.environments.get_id(
          default_environment,
          label='default_environment')
    else:
      self._default_environment_id = None
    self.use_fake_coders = use_fake_coders
    self.iterable_state_read = iterable_state_read
    self.iterable_state_write = iterable_state_write
    self.allow_proto_holders = allow_proto_holders
    self._requirements = set(requirements)

  def add_requirement(self, requirement):
    self._requirements.add(requirement)

  def requirements(self):
    return frozenset(self._requirements)

  # If fake coders are requested, return a pickled version of the element type
  # rather than an actual coder. The element type is required for some runners,
  # as well as performing a round-trip through protos.
  # TODO(BEAM-2717): Remove once this is no longer needed.
  def coder_id_from_element_type(self, element_type: Any) -> str:
    if self.use_fake_coders:
      return pickler.dumps(element_type).decode('ascii')
    else:
      return self.coders.get_id(coders.registry.get_coder(element_type))

  def element_type_from_coder_id(self, coder_id: str) -> Any:
    if self.use_fake_coders or coder_id not in self.coders:
      return pickler.loads(coder_id)
    else:
      return native_type_compatibility.convert_to_beam_type(
          self.coders[coder_id].to_type_hint())

  @staticmethod
  def from_runner_api(proto: beam_runner_api_pb2.Components) -> PipelineContext:
    return PipelineContext(proto)

  def to_runner_api(self) -> beam_runner_api_pb2.Components:
    context_proto = beam_runner_api_pb2.Components()

    self.transforms.populate_map(context_proto.transforms)
    self.pcollections.populate_map(context_proto.pcollections)
    self.coders.populate_map(context_proto.coders)
    self.windowing_strategies.populate_map(context_proto.windowing_strategies)
    self.environments.populate_map(context_proto.environments)

    return context_proto

  def default_environment_id(self) -> Optional[str]:
    return self._default_environment_id
