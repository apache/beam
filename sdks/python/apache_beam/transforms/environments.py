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

import json
import logging
import sys
import tempfile
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import overload

from google.protobuf import message

from apache_beam import coders
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import stager
from apache_beam.runners.portability.sdk_container_builder import SdkContainerImageBuilder
from apache_beam.transforms.resources import resource_hints_from_options
from apache_beam.utils import proto_utils

if TYPE_CHECKING:
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.options.pipeline_options import PortableOptions
  from apache_beam.runners.pipeline_context import PipelineContext

__all__ = [
    'Environment',
    'DefaultEnvironment',
    'DockerEnvironment',
    'ProcessEnvironment',
    'ExternalEnvironment',
    'EmbeddedPythonEnvironment',
    'EmbeddedPythonGrpcEnvironment',
    'SubprocessSDKEnvironment',
    'PyPIArtifactRegistry'
]

T = TypeVar('T')
EnvironmentT = TypeVar('EnvironmentT', bound='Environment')
ConstructorFn = Callable[[
    Optional[Any],
    Iterable[str],
    Iterable[beam_runner_api_pb2.ArtifactInformation],
    Mapping[str, bytes],
    'PipelineContext'
],
                         Any]


def looks_like_json(s):
  import re
  return re.match(r'\s*\{.*\}\s*$', s)


APACHE_BEAM_DOCKER_IMAGE_PREFIX = 'apache/beam'

APACHE_BEAM_JAVA_CONTAINER_NAME_PREFIX = 'beam_java'


def is_apache_beam_container(container_image):
  return container_image and container_image.startswith(
      APACHE_BEAM_DOCKER_IMAGE_PREFIX)


class Environment(object):
  """Abstract base class for environments.

  Represents a type and configuration of environment.
  Each type of Environment should have a unique urn.

  For internal use only. No backwards compatibility guarantees.
  """

  _known_urns = {}  # type: Dict[str, Tuple[Optional[type], ConstructorFn]]
  _urn_to_env_cls = {}  # type: Dict[str, type]

  def __init__(self,
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints=None,  # type: Optional[Mapping[str, bytes]]
               ):
    # type: (...) -> None
    self._capabilities = capabilities
    self._artifacts = sorted(artifacts, key=lambda x: x.SerializeToString())
    # Hints on created environments should be immutable since pipeline context
    # stores environments in hash maps and we use hints to compute the hash.
    self._resource_hints = MappingProxyType(
        dict(resource_hints) if resource_hints else {})

  def __eq__(self, other):
    return (
        self.__class__ == other.__class__ and
        self._artifacts == other._artifacts
        # Assuming that we don't have instances of the same Environment subclass
        # with different set of capabilities.
        and self._resource_hints == other._resource_hints)

  def __hash__(self):
    # type: () -> int
    return hash((self.__class__, frozenset(self._resource_hints.items())))

  def artifacts(self):
    # type: () -> Iterable[beam_runner_api_pb2.ArtifactInformation]
    return self._artifacts

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, Optional[Union[message.Message, bytes, str]]]
    raise NotImplementedError

  def capabilities(self):
    # type: () -> Iterable[str]
    return self._capabilities

  def resource_hints(self):
    # type: () -> Mapping[str, bytes]
    return self._resource_hints

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
    # type: (...) -> Callable[[Union[type, Callable[[bytes, Iterable[str], Iterable[beam_runner_api_pb2.ArtifactInformation], PipelineContext], Any]]], Callable[[bytes, Iterable[str], PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: Type[T]
                   constructor  # type: Callable[[T, Iterable[str], Iterable[beam_runner_api_pb2.ArtifactInformation], PipelineContext], Any]
                  ):
    # type: (...) -> None
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: None
                   constructor  # type: Callable[[bytes, Iterable[str], Iterable[beam_runner_api_pb2.ArtifactInformation], PipelineContext], Any]
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
    # type: (str) -> Type[Environment]
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
        dependencies=self.artifacts(),
        resource_hints=self.resource_hints())

  @classmethod
  def from_runner_api(cls,
                      proto,  # type: Optional[beam_runner_api_pb2.Environment]
                      context  # type: PipelineContext
                     ):
    # type: (...) -> Optional[Environment]
    if proto is None or not proto.urn:
      return None
    parameter_type, constructor = cls._known_urns[proto.urn]

    return constructor(
        proto_utils.parse_Bytes(proto.payload, parameter_type),
        proto.capabilities,
        proto.dependencies,
        proto.resource_hints,
        context)

  @classmethod
  def from_options(cls, options):
    # type: (Type[EnvironmentT], PortableOptions) -> EnvironmentT

    """Creates an Environment object from PortableOptions.

    Args:
      options: The PortableOptions object.
    """
    raise NotImplementedError


@Environment.register_urn(common_urns.environments.DEFAULT.urn, None)
class DefaultEnvironment(Environment):
  """Used as a stub when context is missing a default environment."""
  def to_runner_api_parameter(self, context):
    return common_urns.environments.DEFAULT.urn, None

  @staticmethod
  def from_runner_api_parameter(payload,  # type: beam_runner_api_pb2.DockerPayload
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> DefaultEnvironment
    return DefaultEnvironment(
        capabilities=capabilities,
        artifacts=artifacts,
        resource_hints=resource_hints)


@Environment.register_urn(
    common_urns.environments.DOCKER.urn, beam_runner_api_pb2.DockerPayload)
class DockerEnvironment(Environment):
  def __init__(
      self,
      container_image=None,  # type: Optional[str]
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints=None,  # type: Optional[Mapping[str, bytes]]
  ):
    super().__init__(capabilities, artifacts, resource_hints)
    if container_image:
      logging.info(
          'Using provided Python SDK container image: %s' % (container_image))
      self.container_image = container_image
    else:
      logging.info('No image given, using default Python SDK image')
      self.container_image = self.default_docker_image()

    logging.info(
        'Python SDK container image set to "%s" for Docker environment' %
        (self.container_image))

  def __eq__(self, other):
    return (
        super().__eq__(other) and self.container_image == other.container_image)

  def __hash__(self):
    return hash((super().__hash__(), self.container_image))

  def __repr__(self):
    return 'DockerEnvironment(container_image=%s)' % self.container_image

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, beam_runner_api_pb2.DockerPayload]
    return (
        common_urns.environments.DOCKER.urn,
        beam_runner_api_pb2.DockerPayload(container_image=self.container_image))

  @staticmethod
  def from_runner_api_parameter(payload,  # type: beam_runner_api_pb2.DockerPayload
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> DockerEnvironment
    return DockerEnvironment(
        container_image=payload.container_image,
        capabilities=capabilities,
        artifacts=artifacts,
        resource_hints=resource_hints)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> DockerEnvironment
    if options.view_as(SetupOptions).prebuild_sdk_container_engine:
      prebuilt_container_image = SdkContainerImageBuilder.build_container_image(
          options)
      return cls.from_container_image(
          container_image=prebuilt_container_image,
          artifacts=python_sdk_dependencies(options),
          resource_hints=resource_hints_from_options(options),
      )
    return cls.from_container_image(
        container_image=options.lookup_environment_option(
            'docker_container_image') or options.environment_config,
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options),
    )

  @classmethod
  def from_container_image(
      cls, container_image, artifacts=(), resource_hints=None):
    # type: (str, Iterable[beam_runner_api_pb2.ArtifactInformation], Optional[Mapping[str, bytes]]) -> DockerEnvironment
    return cls(
        container_image=container_image,
        capabilities=python_sdk_docker_capabilities(),
        artifacts=artifacts,
        resource_hints=resource_hints)

  @staticmethod
  def default_docker_image():
    # type: () -> str
    from apache_beam import version as beam_version

    sdk_version = beam_version.__version__
    version_suffix = '.'.join([str(i) for i in sys.version_info[0:2]])

    image = (
        APACHE_BEAM_DOCKER_IMAGE_PREFIX +
        '_python{version_suffix}_sdk:{tag}'.format(
            version_suffix=version_suffix, tag=sdk_version))
    logging.info('Default Python SDK image for environment is %s' % (image))
    return image


@Environment.register_urn(
    common_urns.environments.PROCESS.urn, beam_runner_api_pb2.ProcessPayload)
class ProcessEnvironment(Environment):
  def __init__(
      self,
      command,  # type: str
      os='',  # type: str
      arch='',  # type: str
      env=None,  # type: Optional[Mapping[str, str]]
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints=None,  # type: Optional[Mapping[str, bytes]]
  ):
    # type: (...) -> None
    super().__init__(capabilities, artifacts, resource_hints)
    self.command = command
    self.os = os
    self.arch = arch
    self.env = env or {}

  def __eq__(self, other):
    return (
        super().__eq__(other) and self.command == other.command and
        self.os == other.os and self.arch == other.arch and
        self.env == other.env)

  def __hash__(self):
    # type: () -> int
    return hash((
        super().__hash__(),
        self.command,
        self.os,
        self.arch,
        frozenset(self.env.items())))

  def __repr__(self):
    # type: () -> str
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
  def from_runner_api_parameter(payload,
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> ProcessEnvironment
    return ProcessEnvironment(
        command=payload.command,
        os=payload.os,
        arch=payload.arch,
        env=payload.env,
        capabilities=capabilities,
        artifacts=artifacts,
        resource_hints=resource_hints,
    )

  @staticmethod
  def parse_environment_variables(variables):
    env = {}
    for var in variables:
      try:
        name, value = var.split('=', 1)
        env[name] = value
      except ValueError:
        raise ValueError(
            'Invalid process_variables "%s" (expected assignment in the '
            'form "FOO=bar").' % var)
    return env

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> ProcessEnvironment
    if options.environment_config:
      config = json.loads(options.environment_config)
      return cls(
          config.get('command'),
          os=config.get('os', ''),
          arch=config.get('arch', ''),
          env=config.get('env', ''),
          capabilities=python_sdk_capabilities(),
          artifacts=python_sdk_dependencies(options),
          resource_hints=resource_hints_from_options(options),
      )
    env = cls.parse_environment_variables(
        options.lookup_environment_option('process_variables').split(',')
        if options.lookup_environment_option('process_variables') else [])
    return cls(
        options.lookup_environment_option('process_command'),
        env=env,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options),
    )


@Environment.register_urn(
    common_urns.environments.EXTERNAL.urn, beam_runner_api_pb2.ExternalPayload)
class ExternalEnvironment(Environment):
  def __init__(
      self,
      url,  # type: str
      params=None,  # type: Optional[Mapping[str, str]]
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints=None,  # type: Optional[Mapping[str, bytes]]
  ):
    super().__init__(capabilities, artifacts, resource_hints)
    self.url = url
    self.params = params

  def __eq__(self, other):
    return (
        super().__eq__(other) and self.url == other.url and
        self.params == other.params)

  def __hash__(self):
    # type: () -> int
    return hash((
        super().__hash__(),
        self.url,
        frozenset(self.params.items()) if self.params is not None else None))

  def __repr__(self):
    # type: () -> str
    return 'ExternalEnvironment(url=%s,params=%s)' % (self.url, self.params)

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, beam_runner_api_pb2.ExternalPayload]
    return (
        common_urns.environments.EXTERNAL.urn,
        beam_runner_api_pb2.ExternalPayload(
            endpoint=endpoints_pb2.ApiServiceDescriptor(url=self.url),
            params=self.params))

  @staticmethod
  def from_runner_api_parameter(payload,  # type: beam_runner_api_pb2.ExternalPayload
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> ExternalEnvironment
    return ExternalEnvironment(
        payload.endpoint.url,
        params=payload.params or None,
        capabilities=capabilities,
        artifacts=artifacts,
        resource_hints=resource_hints)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> ExternalEnvironment
    if looks_like_json(options.environment_config):
      config = json.loads(options.environment_config)
      url = config.get('url')
      if not url:
        raise ValueError('External environment endpoint must be set.')
      params = config.get('params')
    elif options.environment_config:
      url = options.environment_config
      params = None
    else:
      url = options.lookup_environment_option('external_service_address')
      params = None

    return cls(
        url,
        params=params,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options))


@Environment.register_urn(python_urns.EMBEDDED_PYTHON, None)
class EmbeddedPythonEnvironment(Environment):
  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, None]
    return python_urns.EMBEDDED_PYTHON, None

  @staticmethod
  def from_runner_api_parameter(unused_payload,  # type: None
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> EmbeddedPythonEnvironment
    return EmbeddedPythonEnvironment(capabilities, artifacts, resource_hints)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> EmbeddedPythonEnvironment
    return cls(
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options),
    )

  @classmethod
  def default(cls):
    # type: () -> EmbeddedPythonEnvironment
    return cls(capabilities=python_sdk_capabilities(), artifacts=())


@Environment.register_urn(python_urns.EMBEDDED_PYTHON_GRPC, bytes)
class EmbeddedPythonGrpcEnvironment(Environment):
  def __init__(
      self,
      state_cache_size=None,
      data_buffer_time_limit_ms=None,
      capabilities=(),
      artifacts=(),
      resource_hints=None,
  ):
    super().__init__(capabilities, artifacts, resource_hints)
    self.state_cache_size = state_cache_size
    self.data_buffer_time_limit_ms = data_buffer_time_limit_ms

  def __eq__(self, other):
    return (
        super().__eq__(other) and
        self.state_cache_size == other.state_cache_size and
        self.data_buffer_time_limit_ms == other.data_buffer_time_limit_ms)

  def __hash__(self):
    # type: () -> int
    return hash((
        super().__hash__(),
        self.state_cache_size,
        self.data_buffer_time_limit_ms))

  def __repr__(self):
    # type: () -> str
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
  def from_runner_api_parameter(payload,  # type: bytes
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> EmbeddedPythonGrpcEnvironment
    if payload:
      config = EmbeddedPythonGrpcEnvironment.parse_config(
          payload.decode('utf-8'))
      return EmbeddedPythonGrpcEnvironment(
          state_cache_size=config.get('state_cache_size'),
          data_buffer_time_limit_ms=config.get('data_buffer_time_limit_ms'),
          capabilities=capabilities,
          artifacts=artifacts,
          resource_hints=resource_hints)
    else:
      return EmbeddedPythonGrpcEnvironment()

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> EmbeddedPythonGrpcEnvironment
    if options.environment_config:
      config = EmbeddedPythonGrpcEnvironment.parse_config(
          options.environment_config)
      return cls(
          state_cache_size=config.get('state_cache_size'),
          data_buffer_time_limit_ms=config.get('data_buffer_time_limit_ms'),
          capabilities=python_sdk_capabilities(),
          artifacts=python_sdk_dependencies(options))
    else:
      return cls(
          capabilities=python_sdk_capabilities(),
          artifacts=python_sdk_dependencies(options),
          resource_hints=resource_hints_from_options(options))

  @staticmethod
  def parse_config(s):
    # type: (str) -> Dict[str, Any]
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

  @classmethod
  def default(cls):
    # type: () -> EmbeddedPythonGrpcEnvironment
    return cls(capabilities=python_sdk_capabilities(), artifacts=())


@Environment.register_urn(python_urns.SUBPROCESS_SDK, bytes)
class SubprocessSDKEnvironment(Environment):
  def __init__(
      self,
      command_string,  # type: str
      capabilities=(),  # type: Iterable[str]
      artifacts=(),  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints=None,  # type: Optional[Mapping[str, bytes]]
  ):
    super().__init__(capabilities, artifacts, resource_hints)
    self.command_string = command_string

  def __eq__(self, other):
    return (
        super().__eq__(other) and self.command_string == other.command_string)

  def __hash__(self):
    # type: () -> int
    return hash((super().__hash__(), self.command_string))

  def __repr__(self):
    # type: () -> str
    return 'SubprocessSDKEnvironment(command_string=%s)' % self.command_string

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> Tuple[str, bytes]
    return python_urns.SUBPROCESS_SDK, self.command_string.encode('utf-8')

  @staticmethod
  def from_runner_api_parameter(payload,  # type: bytes
      capabilities,  # type: Iterable[str]
      artifacts,  # type: Iterable[beam_runner_api_pb2.ArtifactInformation]
      resource_hints,  # type: Mapping[str, bytes]
      context  # type: PipelineContext
                                ):
    # type: (...) -> SubprocessSDKEnvironment
    return SubprocessSDKEnvironment(
        payload.decode('utf-8'), capabilities, artifacts, resource_hints)

  @classmethod
  def from_options(cls, options):
    # type: (PortableOptions) -> SubprocessSDKEnvironment
    return cls(
        options.environment_config,
        capabilities=python_sdk_capabilities(),
        artifacts=python_sdk_dependencies(options),
        resource_hints=resource_hints_from_options(options))

  @classmethod
  def from_command_string(cls, command_string):
    # type: (str) -> SubprocessSDKEnvironment
    return cls(
        command_string, capabilities=python_sdk_capabilities(), artifacts=())


class PyPIArtifactRegistry(object):
  _registered_artifacts = set()  # type: Set[Tuple[str, str]]

  @classmethod
  def register_artifact(cls, name, version):
    cls._registered_artifacts.add((name, version))

  @classmethod
  def get_artifacts(cls):
    for artifact in cls._registered_artifacts:
      yield artifact


def python_sdk_capabilities():
  # type: () -> List[str]
  return list(_python_sdk_capabilities_iter())


def python_sdk_docker_capabilities():
  return python_sdk_capabilities() + [common_urns.protocols.SIBLING_WORKERS.urn]


def _python_sdk_capabilities_iter():
  # type: () -> Iterator[str]
  for urn_spec in common_urns.coders.__dict__.values():
    if getattr(urn_spec, 'urn', None) in coders.Coder._known_urns:
      yield urn_spec.urn
  yield common_urns.protocols.LEGACY_PROGRESS_REPORTING.urn
  yield common_urns.protocols.HARNESS_MONITORING_INFOS.urn
  yield common_urns.protocols.WORKER_STATUS.urn
  yield python_urns.PACKED_COMBINE_FN
  yield 'beam:version:sdk_base:' + DockerEnvironment.default_docker_image()
  yield common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn
  yield common_urns.primitives.TO_STRING.urn


def python_sdk_dependencies(options, tmp_dir=None):
  if tmp_dir is None:
    tmp_dir = tempfile.mkdtemp()
  skip_prestaged_dependencies = options.view_as(
      SetupOptions).prebuild_sdk_container_engine is not None
  return stager.Stager.create_job_resources(
      options,
      tmp_dir,
      pypi_requirements=[
          artifact[0] + artifact[1]
          for artifact in PyPIArtifactRegistry.get_artifacts()
      ],
      skip_prestaged_dependencies=skip_prestaged_dependencies)
