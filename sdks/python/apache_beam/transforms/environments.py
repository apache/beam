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

"""Environments concepts.

For internal use only. No backwards compatibility guarantees."""

# pytype: skip-file

from __future__ import absolute_import

import json
import logging
import sys
import tempfile
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import overload

from google.protobuf import message

from apache_beam import coders
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import stager
from apache_beam.utils import proto_utils

if TYPE_CHECKING:
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.runners.pipeline_context import PipelineContext

__all__ = [
    'Environment',
    'DockerEnvironment',
    'ProcessEnvironment',
    'ExternalEnvironment',
    'EmbeddedPythonEnvironment',
    'EmbeddedPythonGrpcEnvironment',
    'SubprocessSDKEnvironment',
    'RunnerAPIEnvironmentHolder'
]

T = TypeVar('T')
EnvironmentT = TypeVar('EnvironmentT', bound='Environment')
ConstructorFn = Callable[[Optional[Any], Iterable[str], 'PipelineContext'], Any]


def looks_like_json(s):
  import re
  return re.match(r'\s*\{.*\}\s*$', s)


class Environment(object):
  """Abstract base class for environments.

  Represents a type and configuration of environment.
  Each type of Environment should have a unique urn.

  For internal use only. No backwards compatibility guarantees.
  """

  _known_urns = {}  # type: Dict[str, Tuple[Optional[type], ConstructorFn]]
  _urn_to_env_cls = {}  # type: Dict[str, type]

  def __init__(self, capabilities, artifacts):
    self._capabilities = capabilities
    self._artifacts = artifacts

  def artifacts(self):
    # type: () -> Iterable[beam_runner_api_pb2.ArtifactInformation]
    return self._artifacts

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, Optional[Union[message.Message, bytes, str]]]
    raise NotImplementedError

  def capabilities(self):
    # type: () -> Iterable[str]
    return self._capabilities

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: Type[T]
  ):
    # type: (...) -> Callable[[Union[type, Callable[[T, Iterable[str], PipelineContext], Any]]], Callable[[T, Iterable[str], PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: None
  ):
    # type: (...) -> Callable[[Union[type, Callable[[bytes, Iterable[str], PipelineContext], Any]]], Callable[[bytes, Iterable[str], PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: Type[T]
                   constructor  # type: Callable[[T, Iterable[str], PipelineContext], Any]
                  ):
    # type: (...) -> None
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: None
                   constructor  # type: Callable[[bytes, Iterable[str], PipelineContext], Any]
                  ):
    # type: (...) -> None
    pass

  @classmethod
  def register_urn(cls, urn, parameter_type, constructor=None):
    def register(constructor):
      if isinstance(constructor, type):
        constructor.from_runner_api_parameter = register(
            constructor.from_runner_api_parameter)
        # register environment urn to environment class
        cls._urn_to_env_cls[urn] = constructor
        return constructor

      else:
        cls._known_urns[urn] = parameter_type, constructor
        return staticmethod(constructor)

    if constructor:
      # Used as a statement.
      register(constructor)
    else:
      # Used as a decorator.
      return register

  @classmethod
  def get_env_cls_from_urn(cls, urn):
    return cls._urn_to_env_cls[urn]

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.Environment
    urn, typed_param = self.to_runner_api_parameter(context)
    return beam_runner_api_pb2.Environment(
        urn=urn,
        payload=typed_param.SerializeToString() if isinstance(
            typed_param, message.Message) else typed_param if
        (isinstance(typed_param, bytes) or
         typed_param is None) else typed_param.encode('utf-8'),
        capabilities=self.capabilities(),
        dependencies=self.artifacts())

  @classmethod
  def from_runner_api(cls,
                      proto,  # type: Optional[beam_runner_api_pb2.Environment]
                      context  # type: PipelineContext
                     ):
    # type: (...) -> Optional[Environment]
    if proto is None or not proto.urn:
      return None
    parameter_type, constructor = cls._known_urns[proto.urn]

    try:
      return constructor(
          proto_utils.parse_Bytes(proto.payload, parameter_type),
          proto.capabilities,
          proto.dependencies,
          context)
    except Exception:
      if context.allow_proto_holders:
        return RunnerAPIEnvironmentHolder(proto)
      raise

  @classmethod
  def from_options(cls, options):
    # type: (Type[EnvironmentT], PipelineOptions) -> EnvironmentT

    """Creates an Environment object from PipelineOptions.

    Args:
      options: The PipelineOptions object.
    """
    raise NotImplementedError


@Environment.register_urn(
    common_urns.environments.DOCKER.urn, beam_runner_api_pb2.DockerPayload)
class DockerEnvironment(Environment):
  def __init__(self, container_image=None, capabilities=(), artifacts=()):
    super(DockerEnvironment, self).__init__(capabilities, artifacts)
    if container_image:
      self.container_image = container_image
    else:
      self.container_image = self.default_docker_image()

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
           and self.container_image == other.container_image

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((self.__class__, self.container_image))

  def __repr__(self):
    return 'DockerEnvironment(container_image=%s)' % self.container_image

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, beam_runner_api_pb2.DockerPayload]
    return (
        common_urns.environments.DOCKER.urn,
        beam_runner_api_pb2.DockerPayload(container_image=self.container_image))

  @staticmethod
  def from_runner_api_parameter(payload, capabilities, artifacts, context):
    return DockerEnvironment(
        container_image=payload.container_image,
        capabilities=capabilities,
        artifacts=artifacts)

  @classmethod
  def from_options(cls, options):
    # type: (PipelineOptions) -> DockerEnvironment
    return cls.from_container_image(
        container_image=options.environment_config,
        artifacts=python_sdk_dependencies(options))

  @classmethod
  def from_container_image(cls, container_image, artifacts=()):
    # type: (str, Iterable[beam_runner_api_pb2.ArtifactInformation]) -> DockerEnvironment
    return cls(
        container_image=container_image,
        capabilities=python_sdk_capabilities(),
        artifacts=artifacts)

  @staticmethod
  def default_docker_image():
    from apache_beam import version as beam_version

    sdk_version = beam_version.__version__
    version_suffix = '.'.join([str(i) for i in sys.version_info[0:2]])
    logging.warning(
        'Make sure that locally built Python SDK docker image '
        'has Python %d.%d interpreter.' %
        (sys.version_info[0], sys.version_info[1]))

    image = (
        'apache/beam_python{version_suffix}_sdk:{tag}'.format(
            version_suffix=version_suffix, tag=sdk_version))
    logging.info(
        'Using Python SDK docker image: %s. If the image is not '
        'available at local, we will try to pull from hub.docker.com' % (image))
    return image


@Environment.register_urn(
    common_urns.environments.PROCESS.urn, beam_runner_api_pb2.ProcessPayload)
class ProcessEnvironment(Environment):
  def __init__(
      self, command, os='', arch='', env=None, capabilities=(), artifacts=()):
    super(ProcessEnvironment, self).__init__(capabilities, artifacts)
    self.command = command
    self.os = os
    self.arch = arch
    self.env = env or {}

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
      and self.command == other.command and self.os == other.os \
      and self.arch == other.arch and self.env == other.env

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((
        self.__class__,
        self.command,
        self.os,
        self.arch,
        frozenset(self.env.items())))

  def __repr__(self):
    repr_parts = ['command=%s' % self.command]
    if self.os:
      repr_parts.append('os=%s' % self.os)
    if self.arch:
      repr_parts.append('arch=%s' % self.arch)
    repr_parts.append('env=%s' % self.env)
    return 'ProcessEnvironment(%s)' % ','.join(repr_parts)

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, beam_runner_api_pb2.ProcessPayload]
    return (
        common_urns.environments.PROCESS.urn,
        beam_runner_api_pb2.ProcessPayload(
            os=self.os, arch=self.arch, command=self.command, env=self.env))

  @staticmethod
  def from_runner_api_parameter(payload, capabilities, artifacts, context):
    return ProcessEnvironment(
        command=payload.command,
        os=payload.os,
        arch=payload.arch,
        env=payload.env,
        capabilities=capabilities,
        artifacts=artifacts)

  @classmethod
  def from_options(cls, options):
    config = json.loads(options.environment_config)
    return cls(
        config.get('command'),
        os=config.get('os', ''),
        arch=config.get('arch', ''),
        env=config.get('env', ''),
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options))


@Environment.register_urn(
    common_urns.environments.EXTERNAL.urn, beam_runner_api_pb2.ExternalPayload)
class ExternalEnvironment(Environment):
  def __init__(self, url, params=None, capabilities=(), artifacts=()):
    super(ExternalEnvironment, self).__init__(capabilities, artifacts)
    self.url = url
    self.params = params

  def __eq__(self, other):
    return self.__class__ == other.__class__ and self.url == other.url \
      and self.params == other.params

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    params = self.params
    if params is not None:
      params = frozenset(self.params.items())
    return hash((self.__class__, self.url, params))

  def __repr__(self):
    return 'ExternalEnvironment(url=%s,params=%s)' % (self.url, self.params)

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, beam_runner_api_pb2.ExternalPayload]
    return (
        common_urns.environments.EXTERNAL.urn,
        beam_runner_api_pb2.ExternalPayload(
            endpoint=endpoints_pb2.ApiServiceDescriptor(url=self.url),
            params=self.params))

  @staticmethod
  def from_runner_api_parameter(payload, capabilities, artifacts, context):
    return ExternalEnvironment(
        payload.endpoint.url,
        params=payload.params or None,
        capabilities=capabilities,
        artifacts=artifacts)

  @classmethod
  def from_options(cls, options):
    if looks_like_json(options.environment_config):
      config = json.loads(options.environment_config)
      url = config.get('url')
      if not url:
        raise ValueError('External environment endpoint must be set.')
      params = config.get('params')
    else:
      url = options.environment_config
      params = None

    return cls(
        url,
        params=params,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options))


@Environment.register_urn(python_urns.EMBEDDED_PYTHON, None)
class EmbeddedPythonEnvironment(Environment):
  def __init__(self, capabilities=None, artifacts=()):
    super(EmbeddedPythonEnvironment, self).__init__(capabilities, artifacts)

  def __eq__(self, other):
    return self.__class__ == other.__class__

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(self.__class__)

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, None]
    return python_urns.EMBEDDED_PYTHON, None

  @staticmethod
  def from_runner_api_parameter(
      unused_payload, capabilities, artifacts, context):
    return EmbeddedPythonEnvironment(capabilities, artifacts)

  @classmethod
  def from_options(cls, options):
    return cls(
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options))


@Environment.register_urn(python_urns.EMBEDDED_PYTHON_GRPC, bytes)
class EmbeddedPythonGrpcEnvironment(Environment):
  def __init__(
      self,
      state_cache_size=None,
      data_buffer_time_limit_ms=None,
      capabilities=(),
      artifacts=()):
    super(EmbeddedPythonGrpcEnvironment, self).__init__(capabilities, artifacts)
    self.state_cache_size = state_cache_size
    self.data_buffer_time_limit_ms = data_buffer_time_limit_ms

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
           and self.state_cache_size == other.state_cache_size \
           and self.data_buffer_time_limit_ms == other.data_buffer_time_limit_ms

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(
        (self.__class__, self.state_cache_size, self.data_buffer_time_limit_ms))

  def __repr__(self):
    repr_parts = []
    if not self.state_cache_size is None:
      repr_parts.append('state_cache_size=%d' % self.state_cache_size)
    if not self.data_buffer_time_limit_ms is None:
      repr_parts.append(
          'data_buffer_time_limit_ms=%d' % self.data_buffer_time_limit_ms)
    return 'EmbeddedPythonGrpcEnvironment(%s)' % ','.join(repr_parts)

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, bytes]
    params = {}
    if self.state_cache_size is not None:
      params['state_cache_size'] = self.state_cache_size
    if self.data_buffer_time_limit_ms is not None:
      params['data_buffer_time_limit_ms'] = self.data_buffer_time_limit_ms
    payload = json.dumps(params).encode('utf-8')
    return python_urns.EMBEDDED_PYTHON_GRPC, payload

  @staticmethod
  def from_runner_api_parameter(payload, capabilities, artifacts, context):
    if payload:
      config = EmbeddedPythonGrpcEnvironment.parse_config(
          payload.decode('utf-8'))
      return EmbeddedPythonGrpcEnvironment(
          state_cache_size=config.get('state_cache_size'),
          data_buffer_time_limit_ms=config.get('data_buffer_time_limit_ms'),
          capabilities=capabilities,
          artifacts=artifacts)
    else:
      return EmbeddedPythonGrpcEnvironment()

  @classmethod
  def from_options(cls, options):
    if options.environment_config:
      config = EmbeddedPythonGrpcEnvironment.parse_config(
          options.environment_config)
      return cls(
          state_cache_size=config.get('state_cache_size'),
          data_buffer_time_limit_ms=config.get('data_buffer_time_limit_ms'))
    else:
      return cls(
          capabilities=python_sdk_capabilities(),
          artifacts=python_sdk_dependencies(options))

  @staticmethod
  def parse_config(s):
    if looks_like_json(s):
      config_dict = json.loads(s)
      if 'state_cache_size' in config_dict:
        config_dict['state_cache_size'] = int(config_dict['state_cache_size'])

      if 'data_buffer_time_limit_ms' in config_dict:
        config_dict['data_buffer_time_limit_ms'] = \
          int(config_dict['data_buffer_time_limit_ms'])
      return config_dict
    else:
      return {'state_cache_size': int(s)}


@Environment.register_urn(python_urns.SUBPROCESS_SDK, bytes)
class SubprocessSDKEnvironment(Environment):
  def __init__(self, command_string, capabilities=(), artifacts=()):
    super(SubprocessSDKEnvironment, self).__init__(capabilities, artifacts)
    self.command_string = command_string

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
           and self.command_string == other.command_string

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((self.__class__, self.command_string))

  def __repr__(self):
    return 'SubprocessSDKEnvironment(command_string=%s)' % self.command_string

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, bytes]
    return python_urns.SUBPROCESS_SDK, self.command_string.encode('utf-8')

  @staticmethod
  def from_runner_api_parameter(payload, capabilities, artifacts, context):
    return SubprocessSDKEnvironment(
        payload.decode('utf-8'), capabilities, artifacts)

  @classmethod
  def from_options(cls, options):
    return cls(
        options.environment_config,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options))


class RunnerAPIEnvironmentHolder(Environment):
  def __init__(self, proto):
    self.proto = proto

  def to_runner_api(self, context):
    return self.proto

  def capabilities(self):
    return self.proto.capabilities

  def __eq__(self, other):
    return self.__class__ == other.__class__ and self.proto == other.proto

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((self.__class__, self.proto))


def python_sdk_capabilities():
  return list(_python_sdk_capabilities_iter())


def _python_sdk_capabilities_iter():
  for urn_spec in common_urns.coders.__dict__.values():
    if getattr(urn_spec, 'urn', None) in coders.Coder._known_urns:
      yield urn_spec.urn
  yield common_urns.protocols.LEGACY_PROGRESS_REPORTING.urn
  yield common_urns.protocols.WORKER_STATUS.urn
  yield 'beam:version:sdk_base:' + DockerEnvironment.default_docker_image()
  #TODO(BEAM-10530): Add truncate capability.
  # yield common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn


def python_sdk_dependencies(options, tmp_dir=None):
  if tmp_dir is None:
    tmp_dir = tempfile.mkdtemp()
  return tuple(
      beam_runner_api_pb2.ArtifactInformation(
          type_urn=common_urns.artifact_types.FILE.urn,
          type_payload=beam_runner_api_pb2.ArtifactFilePayload(
              path=local_path).SerializeToString(),
          role_urn=common_urns.artifact_roles.STAGING_TO.urn,
          role_payload=beam_runner_api_pb2.ArtifactStagingToRolePayload(
              staged_name=staged_name).SerializeToString()) for local_path,
      staged_name in stager.Stager.create_job_resources(options, tmp_dir))
