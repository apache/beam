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

"""Defines Transform whose expansion is implemented elsewhere.

No backward compatibility guarantees. Everything in this module is experimental.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import contextlib
import copy
import functools
import threading
from typing import Dict

import grpc

from apache_beam import pvalue
from apache_beam.coders import registry
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_expansion_api_pb2
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import artifact_service
from apache_beam.transforms import ptransform
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.trivial_inference import instance_to_type
from apache_beam.typehints.typehints import Union
from apache_beam.typehints.typehints import UnionConstraint
from apache_beam.utils import subprocess_server

DEFAULT_EXPANSION_SERVICE = 'localhost:8097'


def _is_optional_or_none(typehint):
  return (
      type(None) in typehint.union_types if isinstance(
          typehint, UnionConstraint) else typehint is type(None))


def _strip_optional(typehint):
  if not _is_optional_or_none(typehint):
    return typehint
  new_types = typehint.union_types.difference({type(None)})
  if len(new_types) == 1:
    return list(new_types)[0]
  return Union[new_types]


def iter_urns(coder, context=None):
  yield coder.to_runner_api_parameter(context)[0]
  for child in coder._get_component_coders():
    for urn in iter_urns(child, context):
      yield urn


class PayloadBuilder(object):
  """
  Abstract base class for building payloads to pass to ExternalTransform.
  """
  @classmethod
  def _config_value(cls, obj, typehint):
    """
    Helper to create a ConfigValue with an encoded value.
    """
    coder = registry.get_coder(typehint)
    urns = list(iter_urns(coder))
    if 'beam:coder:pickled_python:v1' in urns:
      raise RuntimeError("Found non-portable coder for %s" % (typehint, ))
    return ConfigValue(
        coder_urn=urns, payload=coder.get_impl().encode_nested(obj))

  def build(self):
    """
    :return: ExternalConfigurationPayload
    """
    raise NotImplementedError

  def payload(self):
    """
    The serialized ExternalConfigurationPayload

    :return: bytes
    """
    return self.build().SerializeToString()


class SchemaBasedPayloadBuilder(PayloadBuilder):
  """
  Base class for building payloads based on a schema that provides
  type information for each configuration value to encode.

  Note that if the schema defines a type as Optional, the corresponding value
  will be omitted from the encoded payload, and thus the native transform
  will determine the default.
  """
  def __init__(self, values, schema):
    """
    :param values: mapping of config names to values
    :param schema: mapping of config names to types
    """
    self._values = values
    self._schema = schema

  @classmethod
  def _encode_config(cls, values, schema):
    result = {}
    for key, value in values.items():

      try:
        typehint = schema[key]
      except KeyError:
        raise RuntimeError("No typehint provided for key %r" % key)

      typehint = convert_to_beam_type(typehint)

      if value is None:
        if not _is_optional_or_none(typehint):
          raise RuntimeError(
              "If value is None, typehint should be "
              "optional. Got %r" % typehint)
        # make it easy for user to filter None by default
        continue
      else:
        # strip Optional from typehint so that pickled_python coder is not used
        # for known types.
        typehint = _strip_optional(typehint)
      result[key] = cls._config_value(value, typehint)
    return result

  def build(self):
    """
    :return: ExternalConfigurationPayload
    """
    args = self._encode_config(self._values, self._schema)
    return ExternalConfigurationPayload(configuration=args)


class ImplicitSchemaPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload that generates a schema from the provided values.
  """
  def __init__(self, values):
    schema = {key: instance_to_type(value) for key, value in values.items()}
    super(ImplicitSchemaPayloadBuilder, self).__init__(values, schema)


class NamedTupleBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on a NamedTuple schema.
  """
  def __init__(self, tuple_instance):
    """
    :param tuple_instance: an instance of a typing.NamedTuple
    """
    super(NamedTupleBasedPayloadBuilder, self).__init__(
        values=tuple_instance._asdict(), schema=tuple_instance._field_types)


class AnnotationBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform's type annotations.

  Supported in python 3 only.
  """
  def __init__(self, transform, **values):
    """
    :param transform: a PTransform instance or class. type annotations will
                      be gathered from its __init__ method
    :param values: values to encode
    """
    schema = {
        k: v
        for k,
        v in transform.__init__.__annotations__.items() if k in values
    }
    super(AnnotationBasedPayloadBuilder, self).__init__(values, schema)


class DataclassBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform that uses dataclasses.

  Supported in python 3 only.
  """
  def __init__(self, transform):
    """
    :param transform: a dataclass-decorated PTransform instance from which to
                      gather type annotations and values
    """
    import dataclasses
    schema = {field.name: field.type for field in dataclasses.fields(transform)}
    super(DataclassBasedPayloadBuilder,
          self).__init__(dataclasses.asdict(transform), schema)


class ExternalTransform(ptransform.PTransform):
  """
    External provides a cross-language transform via expansion services in
    foreign SDKs.

    Experimental; no backwards compatibility guarantees.
  """
  _namespace_counter = 0
  _namespace = threading.local()  # type: ignore[assignment]

  _IMPULSE_PREFIX = 'impulse'

  def __init__(self, urn, payload, expansion_service=None):
    """Wrapper for an external transform with the given urn and payload.

    :param urn: the unique beam identifier for this transform
    :param payload: the payload, either as a byte string or a PayloadBuilder
    :param expansion_service: an expansion service implementing the beam
        ExpansionService protocol, either as an object with an Expand method
        or an address (as a str) to a grpc server that provides this method.
    """
    expansion_service = expansion_service or DEFAULT_EXPANSION_SERVICE
    self._urn = urn
    self._payload = (
        payload.payload() if isinstance(payload, PayloadBuilder) else payload)
    self._expansion_service = expansion_service
    self._namespace = self._fresh_namespace()
    self._inputs = {}  # type: Dict[str, pvalue.PCollection]
    self._output = {}  # type: Dict[str, pvalue.PCollection]

  def __post_init__(self, expansion_service):
    """
    This will only be invoked if ExternalTransform is used as a base class
    for a class decorated with dataclasses.dataclass
    """
    ExternalTransform.__init__(
        self, self.URN, DataclassBasedPayloadBuilder(self), expansion_service)

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self._urn)

  @classmethod
  def get_local_namespace(cls):
    return getattr(cls._namespace, 'value', 'external')

  @classmethod
  @contextlib.contextmanager
  def outer_namespace(cls, namespace):
    prev = cls.get_local_namespace()
    cls._namespace.value = namespace
    yield
    cls._namespace.value = prev

  @classmethod
  def _fresh_namespace(cls):
    # type: () -> str
    ExternalTransform._namespace_counter += 1
    return '%s_%d' % (cls.get_local_namespace(), cls._namespace_counter)

  def expand(self, pvalueish):
    # type: (pvalue.PCollection) -> pvalue.PCollection
    if isinstance(pvalueish, pvalue.PBegin):
      self._inputs = {}
    elif isinstance(pvalueish, (list, tuple)):
      self._inputs = {str(ix): pvalue for ix, pvalue in enumerate(pvalueish)}
    elif isinstance(pvalueish, dict):
      self._inputs = pvalueish
    else:
      self._inputs = {'input': pvalueish}
    pipeline = (
        next(iter(self._inputs.values())).pipeline
        if self._inputs else pvalueish.pipeline)
    context = pipeline_context.PipelineContext(
        component_id_map=pipeline.component_id_map)
    transform_proto = beam_runner_api_pb2.PTransform(
        unique_name=pipeline._current_transform().full_label,
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=self._urn, payload=self._payload))
    for tag, pcoll in self._inputs.items():
      transform_proto.inputs[tag] = context.pcollections.get_id(pcoll)
      # Conversion to/from proto assumes producers.
      # TODO: Possibly loosen this.
      context.transforms.put_proto(
          '%s_%s' % (self._IMPULSE_PREFIX, tag),
          beam_runner_api_pb2.PTransform(
              unique_name='%s_%s' % (self._IMPULSE_PREFIX, tag),
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.primitives.IMPULSE.urn),
              outputs={'out': transform_proto.inputs[tag]}))
    components = context.to_runner_api()
    request = beam_expansion_api_pb2.ExpansionRequest(
        components=components,
        namespace=self._namespace,  # type: ignore  # mypy thinks self._namespace is threading.local
        transform=transform_proto)

    with self._service() as service:
      response = service.Expand(request)
      if response.error:
        raise RuntimeError(response.error)
      self._expanded_components = response.components
      if any(env.dependencies
             for env in self._expanded_components.environments.values()):
        self._expanded_components = self._resolve_artifacts(
            self._expanded_components,
            service.artifact_service(),
            pipeline.local_tempdir)

    self._expanded_transform = response.transform
    self._expanded_requirements = response.requirements
    result_context = pipeline_context.PipelineContext(response.components)

    def fix_output(pcoll, tag):
      pcoll.pipeline = pipeline
      pcoll.tag = tag
      return pcoll

    self._outputs = {
        tag: fix_output(result_context.pcollections.get_by_id(pcoll_id), tag)
        for tag,
        pcoll_id in self._expanded_transform.outputs.items()
    }

    return self._output_to_pvalueish(self._outputs)

  @contextlib.contextmanager
  def _service(self):
    if isinstance(self._expansion_service, str):
      channel_options = [("grpc.max_receive_message_length", -1),
                         ("grpc.max_send_message_length", -1)]
      if hasattr(grpc, 'local_channel_credentials'):
        # Some environments may not support insecure channels. Hence use a
        # secure channel with local credentials here.
        # TODO: update this to support secure non-local channels.
        channel_factory_fn = functools.partial(
            grpc.secure_channel,
            self._expansion_service,
            grpc.local_channel_credentials(),
            options=channel_options)
      else:
        # local_channel_credentials is an experimental API which is unsupported
        # by older versions of grpc which may be pulled in due to other project
        # dependencies.
        channel_factory_fn = functools.partial(
            grpc.insecure_channel,
            self._expansion_service,
            options=channel_options)
      with channel_factory_fn() as channel:
        yield ExpansionAndArtifactRetrievalStub(channel)
    elif hasattr(self._expansion_service, 'Expand'):
      yield self._expansion_service
    else:
      with self._expansion_service as stub:
        yield stub

  def _resolve_artifacts(self, components, service, dest):
    for env in components.environments.values():
      if env.dependencies:
        resolved = list(
            artifact_service.resolve_artifacts(env.dependencies, service, dest))
        del env.dependencies[:]
        env.dependencies.extend(resolved)
    return components

  def _output_to_pvalueish(self, output_dict):
    if len(output_dict) == 1:
      return next(iter(output_dict.values()))
    else:
      return output_dict

  def to_runner_api_transform(self, context, full_label):
    pcoll_renames = {}
    renamed_tag_seen = False
    for tag, pcoll in self._inputs.items():
      if tag not in self._expanded_transform.inputs:
        if renamed_tag_seen:
          raise RuntimeError(
              'Ambiguity due to non-preserved tags: %s vs %s' % (
                  sorted(self._expanded_transform.inputs.keys()),
                  sorted(self._inputs.keys())))
        else:
          renamed_tag_seen = True
          tag, = self._expanded_transform.inputs.keys()
      pcoll_renames[self._expanded_transform.inputs[tag]] = (
          context.pcollections.get_id(pcoll))
    for tag, pcoll in self._outputs.items():
      pcoll_renames[self._expanded_transform.outputs[tag]] = (
          context.pcollections.get_id(pcoll))

    def _equivalent(coder1, coder2):
      return coder1 == coder2 or _normalize(coder1) == _normalize(coder2)

    def _normalize(coder_proto):
      normalized = copy.copy(coder_proto)
      normalized.spec.environment_id = ''
      # TODO(robertwb): Normalize components as well.
      return normalized

    for id, proto in self._expanded_components.coders.items():
      if id.startswith(self._namespace):
        context.coders.put_proto(id, proto)
      elif id in context.coders:
        if not _equivalent(context.coders._id_to_proto[id], proto):
          raise RuntimeError(
              'Re-used coder id: %s\n%s\n%s' %
              (id, context.coders._id_to_proto[id], proto))
      else:
        context.coders.put_proto(id, proto)
    for id, proto in self._expanded_components.windowing_strategies.items():
      if id.startswith(self._namespace):
        context.windowing_strategies.put_proto(id, proto)
    for id, proto in self._expanded_components.environments.items():
      if id.startswith(self._namespace):
        context.environments.put_proto(id, proto)
    for id, proto in self._expanded_components.pcollections.items():
      id = pcoll_renames.get(id, id)
      if id not in context.pcollections._id_to_obj.keys():
        context.pcollections.put_proto(id, proto)

    for id, proto in self._expanded_components.transforms.items():
      if id.startswith(self._IMPULSE_PREFIX):
        # Our fake inputs.
        continue
      assert id.startswith(self._namespace), (id, self._namespace)
      new_proto = beam_runner_api_pb2.PTransform(
          unique_name=proto.unique_name,
          spec=proto.spec,
          subtransforms=proto.subtransforms,
          inputs={
              tag: pcoll_renames.get(pcoll, pcoll)
              for tag,
              pcoll in proto.inputs.items()
          },
          outputs={
              tag: pcoll_renames.get(pcoll, pcoll)
              for tag,
              pcoll in proto.outputs.items()
          },
          environment_id=proto.environment_id)
      context.transforms.put_proto(id, new_proto)

    for requirement in self._expanded_requirements:
      context.add_requirement(requirement)

    return beam_runner_api_pb2.PTransform(
        unique_name=full_label,
        spec=self._expanded_transform.spec,
        subtransforms=self._expanded_transform.subtransforms,
        inputs={
            tag: pcoll_renames.get(pcoll, pcoll)
            for tag,
            pcoll in self._expanded_transform.inputs.items()
        },
        outputs={
            tag: pcoll_renames.get(pcoll, pcoll)
            for tag,
            pcoll in self._expanded_transform.outputs.items()
        },
        environment_id=self._expanded_transform.environment_id)


class ExpansionAndArtifactRetrievalStub(
    beam_expansion_api_pb2_grpc.ExpansionServiceStub):
  def __init__(self, channel, **kwargs):
    self._channel = channel
    self._kwargs = kwargs
    super(ExpansionAndArtifactRetrievalStub, self).__init__(channel, **kwargs)

  def artifact_service(self):
    return beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceStub(
        self._channel, **self._kwargs)


class JavaJarExpansionService(object):
  """An expansion service based on an Java Jar file.

  This can be passed into an ExternalTransform as the expansion_service
  argument which will spawn a subprocess using this jar to expand the
  transform.
  """
  def __init__(self, path_to_jar, extra_args=None):
    if extra_args is None:
      extra_args = ['{{PORT}}']
    self._path_to_jar = path_to_jar
    self._extra_args = extra_args
    self._service_count = 0

  def __enter__(self):
    if self._service_count == 0:
      self._path_to_jar = subprocess_server.JavaJarServer.local_jar(
          self._path_to_jar)
      # Consider memoizing these servers (with some timeout).
      self._service_provider = subprocess_server.JavaJarServer(
          ExpansionAndArtifactRetrievalStub,
          self._path_to_jar,
          self._extra_args)
      self._service = self._service_provider.__enter__()
    self._service_count += 1
    return self._service

  def __exit__(self, *args):
    self._service_count -= 1
    if self._service_count == 0:
      self._service_provider.__exit__(*args)


class BeamJarExpansionService(JavaJarExpansionService):
  """An expansion service based on an Beam Java Jar file.

  Attempts to use a locally-built copy of the jar based on the gradle target,
  if it exists, otherwise attempts to download and cache the released artifact
  corresponding to this version of Beam from the apache maven repository.
  """
  def __init__(self, gradle_target, extra_args=None, gradle_appendix=None):
    path_to_jar = subprocess_server.JavaJarServer.path_to_beam_jar(
        gradle_target, gradle_appendix)
    super(BeamJarExpansionService, self).__init__(path_to_jar, extra_args)


def memoize(func):
  cache = {}

  def wrapper(*args):
    if args not in cache:
      cache[args] = func(*args)
    return cache[args]

  return wrapper
