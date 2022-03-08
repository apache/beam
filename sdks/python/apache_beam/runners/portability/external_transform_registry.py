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

"""ExternalTransformRegistry collects and dynamically initializes PTransforms
for expansion service.

No backward compatibility guarantees. Everything in this module is experimental.
"""

from typing import TYPE_CHECKING
from typing import Callable
from typing import Dict
from typing import Optional

from apache_beam.coders import RowCoder
from apache_beam.portability.api import external_transforms_pb2
from apache_beam.portability.common_urns import python_class_lookup
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform

if TYPE_CHECKING:
  from apache_beam.portability.api import beam_runner_api_pb2
  from apache_beam.transforms.ptransform import PTransform


class ExternalTransformRegistry(object):
  _known_urns = {}  # type: Dict[str, Callable[..., PTransform]]

  @classmethod
  def register_urn(cls, urn, constructor=None):
    def register(constructor):
      if isinstance(constructor, Callable):
        cls._known_urns[urn] = constructor
      else:
        raise RuntimeError(
            "only Callable type is allowed but {}".format(type(constructor)))
      return constructor

    if constructor:
      # Used as a statement.
      register(constructor)
    else:
      # Used as a decorator.
      return register

  @classmethod
  def get_registered_transforms(
      cls,
      proto  # type: Optional[beam_runner_api_pb2.PTransform]
  ):
    if proto is None or proto.spec is None or not proto.spec.urn:
      return None

    config_payload = None
    if proto.spec.payload:
      config_payload = external_transforms_pb2.ExternalConfigurationPayload()
      config_payload.ParseFromString(proto.spec.payload)

    if proto.spec.urn == python_class_lookup.urn:
      if config_payload:
        return FullyQualifiedNamedTransform.from_runner_api_parameter(
            None, config_payload, None)
      else:
        return None
    else:
      transform_class = cls._known_urns[proto.spec.urn]
      if config_payload:
        row = RowCoder(config_payload.schema).decode(config_payload.payload)
        return transform_class(**row._asdict())
      else:
        return transform_class()
