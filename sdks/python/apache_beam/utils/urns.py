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

"""For internal use only; no backwards-compatibility guarantees."""

import abc
import inspect

from google.protobuf import wrappers_pb2

from apache_beam.internal import pickler
from apache_beam.utils import proto_utils


class RunnerApiFn(object):
  """Abstract base class that provides urn registration utilities.

  A class that inherits from this class will get a registration-based
  from_runner_api and to_runner_api method that convert to and from
  beam_runner_api_pb2.SdkFunctionSpec.

  Additionally, register_pickle_urn can be called from the body of a class
  to register serialization via pickling.
  """

  # TODO(BEAM-2685): Issue with dill + local classes + abc metaclass
  # __metaclass__ = abc.ABCMeta

  _known_urns = {}

  @abc.abstractmethod
  def to_runner_api_parameter(self, unused_context):
    """Returns the urn and payload for this Fn.

    The returned urn(s) should be registered with `register_urn`.
    """
    pass

  @classmethod
  def register_urn(cls, urn, parameter_type, fn=None):
    """Registers a urn with a constructor.

    For example, if 'beam:fn:foo' had parameter type FooPayload, one could
    write `RunnerApiFn.register_urn('bean:fn:foo', FooPayload, foo_from_proto)`
    where foo_from_proto took as arguments a FooPayload and a PipelineContext.
    This function can also be used as a decorator rather than passing the
    callable in as the final parameter.

    A corresponding to_runner_api_parameter method would be expected that
    returns the tuple ('beam:fn:foo', FooPayload)
    """
    def register(fn):
      cls._known_urns[urn] = parameter_type, fn
      return staticmethod(fn)
    if fn:
      # Used as a statement.
      register(fn)
    else:
      # Used as a decorator.
      return register

  @classmethod
  def register_pickle_urn(cls, pickle_urn):
    """Registers and implements the given urn via pickling.
    """
    inspect.currentframe().f_back.f_locals['to_runner_api_parameter'] = (
        lambda self, context: (
            pickle_urn, wrappers_pb2.BytesValue(value=pickler.dumps(self))))
    cls.register_urn(
        pickle_urn,
        wrappers_pb2.BytesValue,
        lambda proto, unused_context: pickler.loads(proto.value))

  def to_runner_api(self, context):
    """Returns an SdkFunctionSpec encoding this Fn.

    Prefer overriding self.to_runner_api_parameter.
    """
    from apache_beam.portability.api import beam_runner_api_pb2
    urn, typed_param = self.to_runner_api_parameter(context)
    return beam_runner_api_pb2.SdkFunctionSpec(
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=urn,
            payload=typed_param.SerializeToString()
            if typed_param is not None else None))

  @classmethod
  def from_runner_api(cls, fn_proto, context):
    """Converts from an SdkFunctionSpec to a Fn object.

    Prefer registering a urn with its parameter type and constructor.
    """
    parameter_type, constructor = cls._known_urns[fn_proto.spec.urn]
    return constructor(
        proto_utils.parse_Bytes(fn_proto.spec.payload, parameter_type),
        context)
